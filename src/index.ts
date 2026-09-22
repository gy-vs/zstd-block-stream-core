export type FrameHeader={singleSegment:boolean;checksum:boolean,dictionaryIdFlag:number,contentSizeFlag:number};export function parseDescriptor(byte:number):FrameHeader{return{contentSizeFlag:byte>>6,dictionaryIdFlag:byte&3,checksum:Boolean(byte&4),singleSegment:Boolean(byte&32)}}export function readBlockHeader(data:Uint8Array){if(data.length<3)return null;const value=data[0]|data[1]<<8|data[2]<<16;return{last:Boolean(value&1),type:(value>>1)&3,size:value>>3}}

export class BlockStreamError extends Error{
  constructor(message:string){super(message);this.name='BlockStreamError'}
}

export enum BlockType{Raw=0,Rle=1,Compressed=2,Reserved=3}

export type CompressedBlockDecoder=(payload:Uint8Array,maxOutput:number)=>Uint8Array|Promise<Uint8Array>;

export interface BlockStreamOptions{
  /** Declared frame content size. Output may never exceed it and must match it exactly once the last block is done. */
  contentSize?:number;
  /** Hard cap on total decoded bytes, independent of contentSize. */
  maxOutput?:number;
  /** Checksum bytes to consume after the last block (4 when the frame descriptor checksum flag is set). */
  checksumBytes?:number;
  /** Decoder for compressed block payloads. Receives the remaining output budget. */
  decode?:CompressedBlockDecoder;
  /** Maximum bytes produced per yield when expanding RLE blocks. */
  emitChunkSize?:number;
}

export interface BlockStreamSummary{
  blocks:number;
  outputBytes:number;
  checksum:Uint8Array|null;
}

class ChunkReader{
  private queue:Uint8Array[]=[];
  private offset=0;
  private exhausted=false;
  private iterator:AsyncIterator<Uint8Array>|Iterator<Uint8Array>;

  constructor(source:Iterable<Uint8Array>|AsyncIterable<Uint8Array>){
    this.iterator=Symbol.asyncIterator in source
      ?(source as AsyncIterable<Uint8Array>)[Symbol.asyncIterator]()
      :(source as Iterable<Uint8Array>)[Symbol.iterator]();
  }

  get remaining():number{
    let n=-this.offset;
    for(const chunk of this.queue)n+=chunk.length;
    return n;
  }

  async ensure(n:number):Promise<boolean>{
    while(this.remaining<n&&!this.exhausted){
      const{value,done}=await this.iterator.next();
      if(done)this.exhausted=true;
      else if(value&&value.length>0)this.queue.push(value);
    }
    return this.remaining>=n;
  }

  take(n:number):Uint8Array{
    if(n===0)return new Uint8Array(0);
    if(this.remaining<n)throw new BlockStreamError(`cannot take ${n} byte(s), only ${this.remaining} buffered`);
    const head=this.queue[0];
    if(head.length-this.offset>=n){
      const out=head.subarray(this.offset,this.offset+n);
      this.advance(n);
      return out;
    }
    const out=new Uint8Array(n);
    let written=0;
    while(written<n){
      const chunk=this.queue[0];
      const count=Math.min(chunk.length-this.offset,n-written);
      out.set(chunk.subarray(this.offset,this.offset+count),written);
      written+=count;
      this.advance(count);
    }
    return out;
  }

  private advance(n:number):void{
    this.offset+=n;
    if(this.offset===this.queue[0].length){this.queue.shift();this.offset=0}
  }

  async readExactly(n:number,what:string):Promise<Uint8Array>{
    if(!(await this.ensure(n)))
      throw new BlockStreamError(`truncated stream while reading ${what}: wanted ${n} byte(s), ${this.remaining} remaining`);
    return this.take(n);
  }
}

export async function*parseBlockStream(
  source:Iterable<Uint8Array>|AsyncIterable<Uint8Array>,
  options:BlockStreamOptions={},
):AsyncGenerator<Uint8Array,BlockStreamSummary,void>{
  const{contentSize,maxOutput,checksumBytes=0,decode}=options;
  const emitChunkSize=options.emitChunkSize??64*1024;
  if(!Number.isInteger(emitChunkSize)||emitChunkSize<=0)
    throw new BlockStreamError('emitChunkSize must be a positive integer');
  const budget=Math.min(contentSize??Infinity,maxOutput??Infinity);
  const input=new ChunkReader(source);
  let blocks=0;
  let outputBytes=0;

  const charge=(count:number,what:string):void=>{
    if(count>budget-outputBytes)
      throw new BlockStreamError(`${what} exceeds the output budget (${outputBytes} + ${count} > ${budget})`);
    outputBytes+=count;
  };

  for(;;){
    if(!(await input.ensure(3))){
      throw new BlockStreamError(
        input.remaining===0
          ?'stream ended before a last block was seen'
          :`truncated block header: ${input.remaining} of 3 byte(s) available`,
      );
    }
    const header=readBlockHeader(input.take(3))!;

    if(header.type===BlockType.Reserved)
      throw new BlockStreamError('reserved block type 3 is not decodable');

    if(header.type===BlockType.Raw){
      const payload=await input.readExactly(header.size,'raw block payload');
      charge(payload.length,'raw block');
      if(payload.length>0)yield payload;
    }else if(header.type===BlockType.Rle){
      const symbol=(await input.readExactly(1,'RLE block payload'))[0];
      charge(header.size,'RLE block');
      let remaining=header.size;
      while(remaining>0){
        const count=Math.min(remaining,emitChunkSize);
        yield new Uint8Array(count).fill(symbol);
        remaining-=count;
      }
    }else{
      if(!decode)throw new BlockStreamError('compressed block received but no decoder was provided');
      const payload=await input.readExactly(header.size,'compressed block payload');
      const decoded=await decode(payload,budget-outputBytes);
      charge(decoded.length,'decoded block');
      if(decoded.length>0)yield decoded;
    }

    blocks+=1;
    if(header.last)break;
  }

  const checksum=checksumBytes>0?await input.readExactly(checksumBytes,'frame checksum'):null;
  if(contentSize!==undefined&&outputBytes!==contentSize)
    throw new BlockStreamError(`content size mismatch: declared ${contentSize} byte(s), produced ${outputBytes}`);
  return{blocks,outputBytes,checksum};
}
