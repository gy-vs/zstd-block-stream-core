# Zstandard framing core

TypeScript library for Zstandard frame processing.

## Block streams

`parseBlockStream(source, options)` parses a block stream from an (async) iterable of `Uint8Array` chunks and yields decoded bytes. Each block starts with the 3-byte last/type/size header from `readBlockHeader`:

- raw and compressed blocks consume exactly `size` payload bytes; RLE blocks consume a single byte and regenerate `size` bytes, emitted in `emitChunkSize` pieces
- compressed payloads are handed to the injected `decode(payload, maxOutput)` — entropy decoding is out of scope
- total output is capped by `contentSize` / `maxOutput`; after the last block an optional checksum (`checksumBytes`) is consumed before the summary is returned
- parsing is pull-based: input is only read when the consumer asks for more output, so slow sinks apply backpressure, and no block ever reads the next block's bytes

Run `npm install`, then `npm test` and `npm run build`.
