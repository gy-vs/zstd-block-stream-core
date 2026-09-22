/**
 * Zstandard block stream parsing.
 *
 * Block header (little-endian, 3 bytes):
 *   bit 0       last-block flag
 *   bits 1..2   block type (0 raw, 1 RLE, 2 compressed, 3 reserved)
 *   bits 3..23  block content size
 *
 * The parser owns framing only. Compressed payloads are handed to an injected
 * decoder; no entropy decoding is performed here.
 */

export const BlockType = {
  Raw: 0,
  RLE: 1,
  Compressed: 2,
} as const;

export type BlockTypeCode = 0 | 1 | 2 | 3;

export interface ParsedBlockHeader {
  last: boolean;
  type: BlockTypeCode;
  size: number;
}

/** Parse exactly three header bytes. */
export function parseBlockHeader(bytes: Uint8Array): ParsedBlockHeader {
  const word = bytes[0] | (bytes[1] << 8) | (bytes[2] << 16);
  return {
    last: (word & 1) === 1,
    type: ((word >>> 1) & 3) as BlockTypeCode,
    size: word >>> 3,
  };
}

export type BlockStreamErrorCode =
  | 'reserved-block-type'
  | 'payload-truncated'
  | 'missing-last-block'
  | 'checksum-truncated'
  | 'content-overflow'
  | 'decoder-missing'
  | 'decoder-failed'
  | 'decoder-invalid-result'
  | 'sink-failed'
  | 'sink-invalid-result'
  | 'sink-stalled'
  | 'write-after-end'
  | 'already-ended'
  | 'unexpected-data';

export class BlockStreamError extends Error {
  readonly code: BlockStreamErrorCode;
  readonly details?: unknown;

  constructor(code: BlockStreamErrorCode, message: string, details?: unknown) {
    super(message, typeof details === 'object' && details !== null && 'cause' in details ? details : undefined);
    this.name = 'BlockStreamError';
    this.code = code;
    if (details !== undefined) this.details = details;
  }
}

/**
 * Receives decompressed output. Return the number of bytes accepted (partial
 * acceptance and/or a delayed promise apply back-pressure). Returning
 * undefined/void means the whole chunk was accepted. Chunks must not be
 * retained after the call resolves.
 */
export interface BlockSink {
  write(chunk: Uint8Array): number | void | PromiseLike<number | void>;
}

/**
 * Decodes one compressed block. `remainingBudget` is the maximum number of
 * decompressed bytes still allowed; the parser also validates the returned
 * length. The decoder must not exceed the block's declared size, but framing
 * enforces the stream budget here.
 */
export type BlockDecoder = (
  payload: Uint8Array,
  remainingBudget: number,
) => Uint8Array | PromiseLike<Uint8Array>;

export interface BlockStreamOptions {
  sink: BlockSink;
  /** Required when a compressed block is encountered. */
  decoder?: BlockDecoder;
  /** Frame content size from the frame header, when known. */
  contentSize?: number | null;
  /** Output budget used when no frame content size is known (also tightens a known content size). */
  budget?: number | null;
  /** Set when the frame header requests a 4-byte content checksum. */
  checksum?: boolean;
}

export interface BlockStreamSummary {
  complete: true;
  outputSize: number;
  blockCount: number;
  checksum: number | null;
}

type State = 'header' | 'body' | 'checksum' | 'done' | 'error';

/** Append-only queue of byte chunks with logical offsets; no data is copied on push. */
class ByteQueue {
  private chunks: Uint8Array[] = [];
  private offset = 0;
  private total = 0;

  get length(): number {
    return this.total;
  }

  push(chunk: Uint8Array): void {
    if (chunk.length === 0) return;
    this.chunks.push(chunk);
    this.total += chunk.length;
  }

  consume(n: number): void {
    let remaining = n;
    while (remaining > 0) {
      const chunk = this.chunks[0];
      const available = chunk.length - this.offset;
      if (available <= remaining) {
        remaining -= available;
        this.chunks.shift();
        this.offset = 0;
      } else {
        this.offset += remaining;
        remaining = 0;
      }
    }
    this.total -= n;
  }

  /** Copy `n` logical bytes starting at `start` into target. */
  copy(target: Uint8Array, start: number, n: number): void {
    let chunkIndex = 0;
    let position = start + this.offset;
    let written = 0;
    while (written < n) {
      const chunk = this.chunks[chunkIndex];
      const take = Math.min(n - written, chunk.length - position);
      target.set(chunk.subarray(position, position + take), written);
      written += take;
      chunkIndex += 1;
      position = 0;
    }
  }

  /** Logical slices covering [start, start+n) without copying. Caller must consume before mutating input. */
  slices(start: number, n: number): Uint8Array[] {
    const out: Uint8Array[] = [];
    let chunkIndex = 0;
    let position = start + this.offset;
    let remaining = n;
    while (remaining > 0) {
      const chunk = this.chunks[chunkIndex];
      const take = Math.min(remaining, chunk.length - position);
      if (take > 0) out.push(chunk.subarray(position, position + take));
      remaining -= take;
      chunkIndex += 1;
      position = 0;
    }
    return out;
  }
}

const RLE_CHUNK_SIZE = 1 << 16;

async function drainToSink(sink: BlockSink, data: Uint8Array): Promise<void> {
  let offset = 0;
  while (offset < data.length) {
    const part = data.subarray(offset);
    let accepted: number | void;
    try {
      accepted = await sink.write(part);
    } catch (cause) {
      throw new BlockStreamError('sink-failed', 'block sink threw while accepting output', { cause });
    }
    const n = accepted === undefined ? part.length : accepted;
    if (typeof n !== 'number' || !Number.isSafeInteger(n) || n < 0) {
      throw new BlockStreamError(
        'sink-invalid-result',
        'block sink must return a non-negative safe integer, void, or a promise of one',
        { returned: accepted },
      );
    }
    if (offset + n > data.length) {
      throw new BlockStreamError('sink-invalid-result', 'block sink accepted more bytes than offered', {
        accepted: n,
        available: part.length,
      });
    }
    if (n === 0) {
      throw new BlockStreamError(
        'sink-stalled',
        'block sink reported zero accepted bytes; return a promise that resolves when space is available',
      );
    }
    offset += n;
  }
}

export class BlockStreamParser {
  private readonly sink: BlockSink;
  private readonly decoder: BlockDecoder | undefined;
  private readonly outputLimit: number;
  private readonly hasChecksum: boolean;

  private readonly queue = new ByteQueue();
  private readonly headerScratch = new Uint8Array(3);
  private readonly checksumScratch = new Uint8Array(4);

  private state: State = 'header';
  private current: ParsedBlockHeader | null = null;
  private outputCount = 0;
  private blockCount = 0;
  private checksumValue: number | null = null;
  private ended = false;
  private failure: BlockStreamError | null = null;
  private inflight: Promise<void> | null = null;

  constructor(options: BlockStreamOptions) {
    if (!options || typeof options.sink?.write !== 'function') {
      throw new TypeError('BlockStreamParser requires options.sink with a write() method');
    }
    const { contentSize = null, budget = null, checksum = false } = options;
    for (const [name, value] of [
      ['contentSize', contentSize],
      ['budget', budget],
    ] as const) {
      if (value !== null && (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)) {
        throw new TypeError(`${name} must be a non-negative safe integer or null`);
      }
    }
    this.sink = options.sink;
    this.decoder = options.decoder;
    this.hasChecksum = checksum === true;
    if (contentSize === null) this.outputLimit = budget ?? Infinity;
    else if (budget === null) this.outputLimit = contentSize;
    else this.outputLimit = Math.min(contentSize, budget);
  }

  get parserState(): State {
    return this.state;
  }

  get outputSize(): number {
    return this.outputCount;
  }

  get bufferedLength(): number {
    return this.queue.length;
  }

  get blocksParsed(): number {
    return this.blockCount;
  }

  get contentChecksum(): number | null {
    return this.checksumValue;
  }

  /** Feed arbitrary fragment(s) of the block stream. Resolves once buffered data and back-pressure drain. */
  write(chunk: Uint8Array): Promise<void> {
    if (this.ended) {
      return Promise.reject(
        new BlockStreamError('write-after-end', 'cannot write after end() was called'),
      );
    }
    if (this.state === 'done') {
      return Promise.reject(
        new BlockStreamError('unexpected-data', 'data written after the last block was completed'),
      );
    }
    if (this.state === 'error') {
      return Promise.reject(this.failure ?? new BlockStreamError('unexpected-data', 'parser is in error state'));
    }
    this.queue.push(chunk);
    return this.kick();
  }

  /** Mark the stream complete. Resolves with totals or rejects describing why framing is incomplete. */
  async end(): Promise<BlockStreamSummary> {
    if (this.ended) {
      throw new BlockStreamError('already-ended', 'end() called more than once');
    }
    this.ended = true;
    await this.kick();
    if (this.state === 'error') throw this.failure;
    if (this.state === 'done') {
      return {
        complete: true,
        outputSize: this.outputCount,
        blockCount: this.blockCount,
        checksum: this.checksumValue,
      };
    }
    if (this.state === 'checksum') {
      throw new BlockStreamError('checksum-truncated', 'stream ended before the 4-byte content checksum was available', {
        buffered: this.queue.length,
      });
    }
    if (this.state === 'body') {
      throw new BlockStreamError('payload-truncated', 'stream ended in the middle of a block payload', {
        header: this.current,
        buffered: this.queue.length,
      });
    }
    throw new BlockStreamError(
      'missing-last-block',
      this.blockCount === 0
        ? 'stream ended before any block header was complete'
        : 'stream ended without a last block',
      { blockCount: this.blockCount, buffered: this.queue.length },
    );
  }

  private kick(): Promise<void> {
    if (!this.inflight) {
      this.inflight = (async () => {
        try {
          await this.pump();
        } catch (cause) {
          this.state = 'error';
          this.failure =
            cause instanceof BlockStreamError
              ? cause
              : new BlockStreamError('unexpected-data', 'block stream parsing failed', { cause });
        } finally {
          this.inflight = null;
        }
      })();
    }
    return this.inflight.then(() => {
      if (this.failure) throw this.failure;
    });
  }

  private async pump(): Promise<void> {
    for (;;) {
      if (this.state === 'done' || this.state === 'error') return;

      if (this.state === 'header') {
        if (this.queue.length < 3) return;
        this.queue.copy(this.headerScratch, 0, 3);
        const header = parseBlockHeader(this.headerScratch);
        // The 3 header bytes are the only bytes this block consumes at this point.
        this.queue.consume(3);
        if (header.type === 3) {
          throw new BlockStreamError('reserved-block-type', 'block type 3 is reserved', { header });
        }
        this.current = header;
        this.state = 'body';
      }

      if (this.state === 'body') {
        const header = this.current as ParsedBlockHeader;
        // RLE stores exactly one literal byte regardless of the output size.
        const payloadBytes = header.type === BlockType.RLE ? 1 : header.size;
        if (this.queue.length < payloadBytes) return;

        const remaining = this.outputLimit - this.outputCount;
        if (header.type !== BlockType.Compressed && header.size > remaining) {
          throw new BlockStreamError(
            'content-overflow',
            'block output would exceed the frame content size / configured budget',
            { header, remainingBudget: remaining },
          );
        }

        if (header.type === BlockType.Raw) {
          const parts = this.queue.slices(0, header.size);
          for (const part of parts) await drainToSink(this.sink, part);
          this.queue.consume(header.size);
          this.outputCount += header.size;
        } else if (header.type === BlockType.RLE) {
          const repeat = this.queue.slices(0, 1)[0][0];
          // Consume the single stored byte before producing output so back-pressure
          // never causes the next block's bytes to be held as this block's payload.
          this.queue.consume(1);
          if (header.size > 0) {
            let left = header.size;
            while (left > 0) {
              const take = Math.min(RLE_CHUNK_SIZE, left);
              const part = new Uint8Array(take);
              part.fill(repeat);
              await drainToSink(this.sink, part);
              left -= take;
            }
          }
          this.outputCount += header.size;
        } else {
          if (!this.decoder) {
            throw new BlockStreamError('decoder-missing', 'compressed block encountered but no decoder was injected', {
              header,
            });
          }
          const payload = new Uint8Array(header.size);
          if (header.size > 0) this.queue.copy(payload, 0, header.size);
          this.queue.consume(header.size);

          let decoded: Uint8Array;
          try {
            decoded = await this.decoder(payload, remaining);
          } catch (cause) {
            throw new BlockStreamError('decoder-failed', 'injected block decoder failed', { cause, header });
          }
          if (!(decoded instanceof Uint8Array)) {
            throw new BlockStreamError(
              'decoder-invalid-result',
              'block decoder must resolve to a Uint8Array',
              { header },
            );
          }
          if (decoded.length > remaining) {
            throw new BlockStreamError(
              'content-overflow',
              'decoder returned more bytes than the frame content size / configured budget allows',
              { decodedSize: decoded.length, remainingBudget: remaining, header },
            );
          }
          await drainToSink(this.sink, decoded);
          this.outputCount += decoded.length;
        }

        this.blockCount += 1;
        this.current = null;
        if (header.last) {
          this.state = this.hasChecksum ? 'checksum' : 'done';
        } else {
          this.state = 'header';
        }
        continue;
      }

      if (this.state === 'checksum') {
        if (this.queue.length < 4) return;
        this.queue.copy(this.checksumScratch, 0, 4);
        this.queue.consume(4);
        this.checksumValue =
          (this.checksumScratch[0] |
          (this.checksumScratch[1] << 8) |
          (this.checksumScratch[2] << 16) |
          (this.checksumScratch[3] << 24)) >>> 0;
        this.state = 'done';
        return;
      }
    }
  }
}
