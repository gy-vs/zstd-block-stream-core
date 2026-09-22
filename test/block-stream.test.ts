import { describe, expect, it } from 'vitest';
import {
  BlockStreamError,
  BlockStreamParser,
  BlockType,
  type BlockSink,
} from '../src/index.js';

function blockHeader(last: boolean, type: number, size: number): Uint8Array {
  const word = (last ? 1 : 0) | (type << 1) | (size << 3);
  return Uint8Array.from([word & 0xff, (word >> 8) & 0xff, (word >> 16) & 0xff]);
}

function bytes(...values: number[]): Uint8Array {
  return Uint8Array.from(values);
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

class CollectSink implements BlockSink {
  readonly chunks: Uint8Array[] = [];
  readonly events: number[] = [];
  /** When >0, each write accepts at most this many bytes. */
  maxPerWrite = Infinity;
  /** When set, writes block until the gate resolves (once per write call). */
  gate: (() => Promise<void>) | null = null;

  async write(chunk: Uint8Array): Promise<number | void> {
    if (this.gate !== null) {
      const wait = this.gate;
      this.gate = null;
      await wait();
    }
    const take = Math.min(chunk.length, this.maxPerWrite);
    const copy = chunk.slice(0, take);
    this.chunks.push(copy);
    this.events.push(copy.length);
    if (take === chunk.length) return;
    return take;
  }

  get output(): Uint8Array {
    return concat(...this.chunks);
  }
}

describe('block stream parsing', () => {
  it('parses several blocks: raw, RLE and injected-decoder compressed, with exact byte boundaries', async () => {
    const sink = new CollectSink();
    const raw1 = bytes(1, 2, 3, 4, 5);
    const rleByte = bytes(0x7a);
    const compressedPayload = bytes(10, 20, 30);
    const raw2 = bytes(9, 8, 7, 6);

    const stream = concat(
      blockHeader(false, BlockType.Raw, raw1.length),
      raw1,
      blockHeader(false, BlockType.RLE, 7),
      rleByte,
      blockHeader(false, BlockType.Compressed, compressedPayload.length),
      compressedPayload,
      blockHeader(true, BlockType.Raw, raw2.length),
      raw2,
    );

    let seenPayload: Uint8Array | null = null;
    const parser = new BlockStreamParser({
      sink,
      contentSize: 5 + 7 + 3 + 4,
      decoder: (payload) => {
        seenPayload = payload.slice();
        return bytes(100, 101, 102);
      },
    });

    await parser.write(stream);
    const summary = await parser.end();

    expect(seenPayload).toEqual(compressedPayload);
    expect(summary.complete).toBe(true);
    expect(summary.blockCount).toBe(4);
    expect(summary.outputSize).toBe(19);
    expect(Array.from(sink.output)).toEqual([
      1, 2, 3, 4, 5,
      0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a,
      100, 101, 102,
      9, 8, 7, 6,
    ]);
  });

  it('accepts an empty raw last block', async () => {
    const sink = new CollectSink();
    const parser = new BlockStreamParser({ sink, contentSize: 0 });
    await parser.write(blockHeader(true, BlockType.Raw, 0));
    const summary = await parser.end();
    expect(summary.outputSize).toBe(0);
    expect(summary.blockCount).toBe(1);
    expect(sink.chunks).toHaveLength(0);
  });

  it('RLE block with size 0 still consumes exactly one stored byte and outputs nothing', async () => {
    const sink = new CollectSink();
    const parser = new BlockStreamParser({ sink, contentSize: 0 });
    // Header + exactly one payload byte; the parser must read it.
    await parser.write(concat(blockHeader(true, BlockType.RLE, 0), bytes(0x42)));
    const summary = await parser.end();
    expect(summary.outputSize).toBe(0);
    expect(summary.blockCount).toBe(1);
    expect(parser.bufferedLength).toBe(0);
  });

  it('produces large RLE output from a single byte under a slow, partially-accepting sink', async () => {
    const sink = new CollectSink();
    sink.maxPerWrite = 4096;
    const size = 200_003;
    const parser = new BlockStreamParser({ sink, contentSize: size });
    await parser.write(concat(blockHeader(true, BlockType.RLE, size), bytes(0xc3)));
    const summary = await parser.end();

    expect(summary.outputSize).toBe(size);
    expect(summary.blockCount).toBe(1);
    const out = sink.output;
    expect(out.length).toBe(size);
    for (const value of out) expect(value).toBe(0xc3);
    // Every sink event was at most the advertised chunk.
    expect(sink.events.every((n) => n <= 4096)).toBe(true);
  });

  it('never lets a block read into the next block while a slow sink applies back-pressure', async () => {
    const sink = new CollectSink();
    const rleSize = 100_000;
    const nextPayload = bytes(1, 2, 3, 4, 5, 6);
    const wire = concat(
      blockHeader(false, BlockType.RLE, rleSize),
      bytes(0x55),
      blockHeader(true, BlockType.Raw, nextPayload.length),
      nextPayload,
    );

    let release: (() => void) | null = null;
    sink.gate = () =>
      new Promise<void>((resolve) => {
        release = resolve;
      });

    const parser = new BlockStreamParser({ sink, contentSize: rleSize + 6 });
    const pending = parser.write(wire);
    await Promise.resolve();
    await Promise.resolve();
    expect(release).not.toBeNull();
    // While blocked: the single RLE byte is consumed, but every byte of the
    // following block is still buffered and untouched.
    expect(parser.bufferedLength).toBe(3 + nextPayload.length);
    expect(parser.blocksParsed).toBe(0);

    release!();
    await pending;
    const summary = await parser.end();
    expect(summary.outputSize).toBe(rleSize + 6);
    expect(Array.from(sink.output.slice(rleSize))).toEqual([1, 2, 3, 4, 5, 6]);
    expect(parser.bufferedLength).toBe(0);
  });

  it('rejects reserved block type 3 without consuming payload bytes', async () => {
    const sink = new CollectSink();
    const parser = new BlockStreamParser({ sink });
    await expect(
      parser.write(concat(blockHeader(false, 3, 123), bytes(1, 2, 3))),
    ).rejects.toMatchObject({ code: 'reserved-block-type' });
    expect(parser.parserState).toBe('error');
    // Header (3 bytes) was consumed; alleged payload remains buffered.
    expect(parser.bufferedLength).toBe(3);
    await expect(parser.end()).rejects.toBeInstanceOf(BlockStreamError);
  });

  it('reports payload truncation for raw and RLE blocks', async () => {
    {
      const sink = new CollectSink();
      const parser = new BlockStreamParser({ sink, contentSize: 10 });
      await parser.write(concat(blockHeader(true, BlockType.Raw, 10), bytes(1, 2, 3, 4)));
      await expect(parser.end()).rejects.toMatchObject({ code: 'payload-truncated' });
    }
    {
      const sink = new CollectSink();
      const parser = new BlockStreamParser({ sink, contentSize: 10 });
      // RLE always needs its single literal byte.
      await parser.write(blockHeader(true, BlockType.RLE, 10));
      await expect(parser.end()).rejects.toMatchObject({ code: 'payload-truncated' });
    }
  });

  it('reports a truncated compressed payload and does not invoke the decoder', async () => {
    let called = 0;
    const parser = new BlockStreamParser({
      sink: new CollectSink(),
      decoder: () => {
        called += 1;
        return new Uint8Array(0);
      },
    });
    await parser.write(concat(blockHeader(true, BlockType.Compressed, 8), bytes(1, 2, 3)));
    await expect(parser.end()).rejects.toMatchObject({ code: 'payload-truncated' });
    expect(called).toBe(0);
  });

  it('reports missing last block when the stream ends after a non-last block', async () => {
    const sink = new CollectSink();
    const parser = new BlockStreamParser({ sink, contentSize: 3 });
    await parser.write(concat(blockHeader(false, BlockType.Raw, 3), bytes(7, 8, 9)));
    await expect(parser.end()).rejects.toMatchObject({ code: 'missing-last-block' });
  });

  it('reports missing last block when not even one header is complete', async () => {
    const parser = new BlockStreamParser({ sink: new CollectSink() });
    await parser.write(bytes(0, 1));
    await expect(parser.end()).rejects.toMatchObject({ code: 'missing-last-block' });
  });

  it('propagates synchronous and asynchronous decoder failures with exact payload', async () => {
    for (const fail of [
      () => {
        throw new Error('boom');
      },
      async () => {
        throw new Error('async boom');
      },
    ]) {
      const payload = bytes(4, 5, 6, 7);
      let seen: Uint8Array | null = null;
      const parser = new BlockStreamParser({
        sink: new CollectSink(),
        decoder: (data) => {
          seen = data.slice();
          return fail();
        },
      });
      await expect(
        parser.write(concat(blockHeader(true, BlockType.Compressed, 4), payload)),
      ).rejects.toMatchObject({ code: 'decoder-failed' });
      expect(seen).toEqual(payload);
      expect(parser.parserState).toBe('error');
    }
  });

  it('requires a decoder for compressed blocks', async () => {
    const parser = new BlockStreamParser({ sink: new CollectSink() });
    await expect(
      parser.write(concat(blockHeader(true, BlockType.Compressed, 1), bytes(0))),
    ).rejects.toMatchObject({ code: 'decoder-missing' });
  });

  it('rejects decoder output exceeding the budget / frame content size', async () => {
    const parser = new BlockStreamParser({
      sink: new CollectSink(),
      contentSize: 2,
      decoder: () => bytes(9, 9, 9),
    });
    await expect(
      parser.write(concat(blockHeader(true, BlockType.Compressed, 1), bytes(0))),
    ).rejects.toMatchObject({ code: 'content-overflow' });
  });

  it('enforces the cumulative budget for raw and RLE blocks', async () => {
    {
      const parser = new BlockStreamParser({ sink: new CollectSink(), budget: 4 });
      await expect(
        parser.write(concat(blockHeader(true, BlockType.Raw, 5), new Uint8Array(5))),
      ).rejects.toMatchObject({ code: 'content-overflow' });
    }
    {
      const parser = new BlockStreamParser({ sink: new CollectSink(), budget: 9 });
      await parser.write(concat(blockHeader(false, BlockType.Raw, 5), bytes(1, 2, 3, 4, 5)));
      await expect(
        parser.write(concat(blockHeader(true, BlockType.RLE, 5), bytes(0xff))),
      ).rejects.toMatchObject({ code: 'content-overflow' });
    }
  });

  it('strictly consumes raw size bytes across one-byte fragments', async () => {
    const sink = new CollectSink();
    const parser = new BlockStreamParser({ sink, contentSize: 5 });
    const wire = concat(blockHeader(true, BlockType.Raw, 5), bytes(11, 22, 33, 44, 55));
    for (const byte of wire) await parser.write(bytes(byte));
    const summary = await parser.end();
    expect(summary.blockCount).toBe(1);
    expect(Array.from(sink.output)).toEqual([11, 22, 33, 44, 55]);
    expect(parser.bufferedLength).toBe(0);
  });

  it('parses block boundaries when two blocks arrive in one fragmented write', async () => {
    const sink = new CollectSink();
    const parser = new BlockStreamParser({ sink, contentSize: 7 });
    const wire = concat(
      blockHeader(false, BlockType.Raw, 4),
      bytes(1, 1, 1, 1),
      blockHeader(true, BlockType.Raw, 3),
      bytes(2, 2, 2),
    );
    await parser.write(wire.subarray(0, 6)); // header + 3 of first payload
    expect(parser.blocksParsed).toBe(0);
    await parser.write(wire.subarray(6));
    const summary = await parser.end();
    expect(summary.blockCount).toBe(2);
    expect(Array.from(sink.output)).toEqual([1, 1, 1, 1, 2, 2, 2]);
  });

  it('enters checksum state after last and reads the little-endian checksum', async () => {
    const sink = new CollectSink();
    const parser = new BlockStreamParser({ sink, contentSize: 2, checksum: true });
    await parser.write(concat(blockHeader(true, BlockType.Raw, 2), bytes(0xaa, 0xbb)));
    expect(parser.parserState).toBe('checksum');
    // While the checksum is incomplete, end reports truncation.
    const parser2 = new BlockStreamParser({ sink: new CollectSink(), checksum: true });
    await parser2.write(concat(blockHeader(true, BlockType.Raw, 0), bytes(1, 2)));
    await expect(parser2.end()).rejects.toMatchObject({ code: 'checksum-truncated' });

    await parser.write(bytes(0xde, 0xad, 0xbe, 0xef));
    const summary = await parser.end();
    expect(summary.checksum).toBe(0xefbeadde);
    expect(summary.outputSize).toBe(2);
  });

  it('completes without checksum when the frame does not request one', async () => {
    const parser = new BlockStreamParser({ sink: new CollectSink() });
    await parser.write(blockHeader(true, BlockType.Raw, 0));
    const summary = await parser.end();
    expect(summary.checksum).toBeNull();
    expect(summary.complete).toBe(true);
  });

  it('rejects writes after end and after completion', async () => {
    const parser = new BlockStreamParser({ sink: new CollectSink() });
    await parser.write(blockHeader(true, BlockType.Raw, 0));
    // Extra data before end() is flagged as data past the last block.
    await expect(parser.write(bytes(0))).rejects.toMatchObject({ code: 'unexpected-data' });
    await parser.end();
    await expect(parser.write(bytes(0))).rejects.toMatchObject({ code: 'write-after-end' });
    await expect(parser.end()).rejects.toMatchObject({ code: 'already-ended' });
  });

  it('surfaces sink failures and invalid sink return values', async () => {
    const boom: BlockSink = {
      write() {
        throw new Error('disk gone');
      },
    };
    const p1 = new BlockStreamParser({ sink: boom });
    await expect(
      p1.write(concat(blockHeader(true, BlockType.Raw, 1), bytes(1))),
    ).rejects.toMatchObject({ code: 'sink-failed' });

    const badReturn: BlockSink = {
      write: () => -1 as unknown as number,
    };
    const p2 = new BlockStreamParser({ sink: badReturn });
    await expect(
      p2.write(concat(blockHeader(true, BlockType.Raw, 1), bytes(1))),
    ).rejects.toMatchObject({ code: 'sink-invalid-result' });

    const zero: BlockSink = { write: () => 0 };
    const p3 = new BlockStreamParser({ sink: zero });
    await expect(
      p3.write(concat(blockHeader(true, BlockType.Raw, 1), bytes(1))),
    ).rejects.toMatchObject({ code: 'sink-stalled' });
  });
});
