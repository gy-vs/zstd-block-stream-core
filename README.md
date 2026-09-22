# Zstandard framing core

TypeScript library for Zstandard frame processing.

Run `npm install`, then `npm test` and `npm run build`.

## Block stream parsing

`BlockStreamParser` parses a stream of Zstandard blocks. It handles framing
only: raw blocks are forwarded, RLE blocks expanded, and compressed block
payloads are handed to an injected decoder (no entropy decoding here).

Block header (3 bytes, little-endian):

| bits | field |
| --- | --- |
| 0 | last block flag |
| 1–2 | type: `0` raw, `1` RLE, `2` compressed, `3` reserved |
| 3–23 | block content size |

Consumption rules:

- **Raw**: exactly `size` payload bytes are consumed and forwarded to the sink.
- **RLE**: exactly one literal byte is consumed; the sink receives `size`
  bytes (emitted in chunks, honoring sink back-pressure).
- **Compressed**: exactly `size` payload bytes are copied and passed to
  `decoder(payload, remainingBudget)`; the decoded length is then validated.
- A block never consumes bytes belonging to the following block. After the
  last block the parser moves to the checksum state (when enabled) or
  completes.

```ts
import { BlockStreamParser, BlockType } from './dist/index.js';

const parser = new BlockStreamParser({
  sink: { write: async (chunk) => process(chunk) }, // return accepted count for back-pressure
  decoder: async (payload, remainingBudget) => decodeBlock(payload, remainingBudget),
  contentSize: 1024, // optional, bounds cumulative output
  budget: 4096,      // optional, used / tightened when no contentSize
  checksum: false,   // read 4-byte content checksum after the last block
});

await parser.write(chunk);   // any fragment boundaries
const result = await parser.end();
// { complete: true, outputSize, blockCount, checksum }
```

Errors are `BlockStreamError` instances with codes such as
`reserved-block-type`, `payload-truncated`, `missing-last-block`,
`checksum-truncated`, `content-overflow`, `decoder-missing`,
`decoder-failed`, and `sink-failed`.
