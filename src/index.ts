export type FrameHeader={singleSegment:boolean;checksum:boolean,dictionaryIdFlag:number,contentSizeFlag:number};export function parseDescriptor(byte:number):FrameHeader{return{contentSizeFlag:byte>>6,dictionaryIdFlag:byte&3,checksum:Boolean(byte&4),singleSegment:Boolean(byte&32)}}
export {
  BlockType,
  BlockStreamError,
  BlockStreamParser,
  parseBlockHeader,
} from './block-stream.js';
export type {
  BlockDecoder,
  BlockSink,
  BlockStreamOptions,
  BlockStreamSummary,
  BlockTypeCode,
  ParsedBlockHeader,
} from './block-stream.js';
import {parseBlockHeader} from './block-stream.js';

/** @deprecated Use parseBlockHeader; returns null when fewer than 3 bytes are available. */
export function readBlockHeader(data:Uint8Array){if(data.length<3)return null;return parseBlockHeader(data.subarray(0,3));}
