import{expect,it}from'vitest';
import{BlockType,parseBlockStream,type BlockStreamSummary}from'../src/index.js';

function header(last:boolean,type:BlockType,size:number):Uint8Array{
  const v=(last?1:0)|(type<<1)|(size<<3);
  return new Uint8Array([v&0xff,(v>>8)&0xff,(v>>16)&0xff]);
}

function bytes(...values:number[]):Uint8Array{
  return new Uint8Array(values);
}

function concat(...parts:Uint8Array[]):Uint8Array{
  const out=new Uint8Array(parts.reduce((n,p)=>n+p.length,0));
  let offset=0;
  for(const p of parts){out.set(p,offset);offset+=p.length}
  return out;
}

function text(s:string):Uint8Array{
  return bytes(...s.split('').map(c=>c.charCodeAt(0)));
}

async function collect(gen:AsyncGenerator<Uint8Array,BlockStreamSummary>){
  const parts:Uint8Array[]=[];
  let result=await gen.next();
  while(!result.done){parts.push(result.value);result=await gen.next()}
  return{output:concat(...parts),summary:result.value};
}

async function*chunkwise(data:Uint8Array,size:number):AsyncGenerator<Uint8Array>{
  for(let i=0;i<data.length;i+=size)yield data.subarray(i,i+size);
}

it('parses an empty raw block',async()=>{
  const{output,summary}=await collect(parseBlockStream([header(true,BlockType.Raw,0)]));
  expect(output.length).toBe(0);
  expect(summary).toMatchObject({blocks:1,outputBytes:0,checksum:null});
});

it('handles a zero-size RLE block',async()=>{
  const stream=concat(header(false,BlockType.Rle,0),bytes(0x00),header(true,BlockType.Raw,1),bytes(0x2a));
  const{output,summary}=await collect(parseBlockStream([stream]));
  expect([...output]).toEqual([0x2a]);
  expect(summary.blocks).toBe(2);
});

it('concatenates multiple blocks without reading across block boundaries',async()=>{
  const stream=concat(
    header(false,BlockType.Raw,5),text('hello'),
    header(false,BlockType.Rle,3),bytes(0x61),
    header(true,BlockType.Raw,1),bytes(0x21),
  );
  // the whole stream arrives as a single chunk: each block must stop exactly at its own boundary
  const{output,summary}=await collect(parseBlockStream([stream]));
  expect(String.fromCharCode(...output)).toBe('helloaaa!');
  expect(summary.blocks).toBe(3);
  expect(summary.outputBytes).toBe(9);
});

it('handles input delivered one byte at a time',async()=>{
  const stream=concat(header(false,BlockType.Raw,2),bytes(1,2),header(true,BlockType.Raw,2),bytes(3,4));
  const{output}=await collect(parseBlockStream(chunkwise(stream,1)));
  expect([...output]).toEqual([1,2,3,4]);
});

it('assembles raw payloads split across input chunks',async()=>{
  const stream=concat(header(true,BlockType.Raw,6),bytes(1,2,3,4,5,6));
  const{output}=await collect(parseBlockStream(chunkwise(stream,4)));
  expect([...output]).toEqual([1,2,3,4,5,6]);
});

it('expands a large RLE block incrementally from a single payload byte',async()=>{
  const size=1_000_000;
  // the source provides exactly one payload byte; reading `size` bytes would truncate
  const gen=parseBlockStream([concat(header(true,BlockType.Rle,size),bytes(0x7f))],{emitChunkSize:4096});
  let total=0,yields=0,max=0;
  let result=await gen.next();
  while(!result.done){
    yields++;total+=result.value.length;max=Math.max(max,result.value.length);
    expect(result.value.every(b=>b===0x7f)).toBe(true);
    result=await gen.next();
  }
  expect(total).toBe(size);
  expect(yields).toBeGreaterThan(1);
  expect(max).toBeLessThanOrEqual(4096);
  expect(result.value.outputBytes).toBe(size);
});

it('rejects the reserved block type without touching payload bytes',async()=>{
  // only the 3 header bytes exist; trying to read a payload would fail with truncation instead
  const gen=parseBlockStream([header(true,BlockType.Reserved,5)]);
  await expect(collect(gen)).rejects.toThrow(/reserved block type/);
});

it('fails on a truncated raw payload',async()=>{
  const gen=parseBlockStream([concat(header(true,BlockType.Raw,5),bytes(1,2,3))]);
  await expect(collect(gen)).rejects.toThrow(/truncated stream while reading raw block payload/);
});

it('fails on a truncated RLE payload',async()=>{
  const gen=parseBlockStream([header(true,BlockType.Rle,10)]);
  await expect(collect(gen)).rejects.toThrow(/truncated stream while reading RLE block payload/);
});

it('fails on a truncated block header',async()=>{
  const gen=parseBlockStream([bytes(0,1)]);
  await expect(collect(gen)).rejects.toThrow(/truncated block header/);
});

it('fails when the stream ends before a last block',async()=>{
  const gen=parseBlockStream([concat(header(false,BlockType.Raw,1),bytes(42))]);
  await expect(collect(gen)).rejects.toThrow(/before a last block/);
});

it('hands compressed payloads to the injected decoder, strictly size bytes each',async()=>{
  const seen:number[]=[];
  const decode=(payload:Uint8Array)=>{seen.push(payload.length);return Uint8Array.from(payload).reverse()};
  const stream=concat(
    header(false,BlockType.Compressed,3),bytes(1,2,3),
    header(true,BlockType.Compressed,2),bytes(4,5),
  );
  const{output,summary}=await collect(parseBlockStream([stream],{decode}));
  expect([...output]).toEqual([3,2,1,5,4]);
  expect(seen).toEqual([3,2]);
  expect(summary.outputBytes).toBe(5);
});

it('supports asynchronous decoders',async()=>{
  const decode=async(payload:Uint8Array)=>Uint8Array.from(payload).reverse();
  const stream=concat(header(true,BlockType.Compressed,2),bytes(1,2));
  const{output}=await collect(parseBlockStream([stream],{decode}));
  expect([...output]).toEqual([2,1]);
});

it('rejects compressed blocks when no decoder is configured',async()=>{
  const gen=parseBlockStream([concat(header(true,BlockType.Compressed,1),bytes(0))]);
  await expect(collect(gen)).rejects.toThrow(/no decoder/);
});

it('propagates decoder failures',async()=>{
  const failure=new Error('entropy decode exploded');
  const gen=parseBlockStream([concat(header(true,BlockType.Compressed,2),bytes(1,2))],{
    decode:()=>{throw failure},
  });
  await expect(collect(gen)).rejects.toBe(failure);
});

it('passes the remaining output budget to the decoder',async()=>{
  let observed=-1;
  const decode=(_payload:Uint8Array,maxOutput:number)=>{observed=maxOutput;return bytes(1,2)};
  const stream=concat(header(false,BlockType.Raw,3),bytes(7,7,7),header(true,BlockType.Compressed,1),bytes(0));
  await collect(parseBlockStream([stream],{decode,maxOutput:10}));
  expect(observed).toBe(7);
});

it('enforces the output budget on RLE expansion',async()=>{
  const gen=parseBlockStream([concat(header(true,BlockType.Rle,100),bytes(1))],{maxOutput:50});
  await expect(collect(gen)).rejects.toThrow(/output budget/);
});

it('enforces the budget on decoder output',async()=>{
  const decode=()=>bytes(1,2,3,4);
  const gen=parseBlockStream([concat(header(true,BlockType.Compressed,2),bytes(9,9))],{decode,contentSize:3});
  await expect(collect(gen)).rejects.toThrow(/output budget/);
});

it('verifies the declared content size at the end of the frame',async()=>{
  const ok=parseBlockStream([concat(header(true,BlockType.Raw,2),bytes(1,2))],{contentSize:2});
  await expect(collect(ok)).resolves.toMatchObject({summary:{outputBytes:2}});
  const short=parseBlockStream([concat(header(true,BlockType.Raw,2),bytes(1,2))],{contentSize:5});
  await expect(collect(short)).rejects.toThrow(/content size mismatch/);
});

it('consumes the checksum after the last block',async()=>{
  const stream=concat(header(true,BlockType.Raw,1),bytes(0xaa),bytes(0xde,0xad,0xbe,0xef));
  const{output,summary}=await collect(parseBlockStream([stream],{checksumBytes:4}));
  expect([...output]).toEqual([0xaa]);
  expect([...summary.checksum!]).toEqual([0xde,0xad,0xbe,0xef]);
});

it('fails on a truncated checksum',async()=>{
  const stream=concat(header(true,BlockType.Raw,0),bytes(1,2));
  await expect(collect(parseBlockStream([stream],{checksumBytes:4}))).rejects.toThrow(/truncated stream while reading frame checksum/);
});

it('respects backpressure from a slow sink and never runs ahead of it',async()=>{
  let produced=0;
  const source=(async function*(){
    produced++;
    yield concat(header(false,BlockType.Rle,8),bytes(0x55));
    produced++;
    yield concat(header(true,BlockType.Raw,1),bytes(0x99));
  })();

  const gen=parseBlockStream(source,{emitChunkSize:2});
  const received:number[]=[];
  let result=await gen.next();
  // the first RLE output chunk is produced from the first input chunk alone
  expect(produced).toBe(1);
  while(!result.done){
    await new Promise(resolve=>setTimeout(resolve,1)); // slow sink
    received.push(...result.value);
    result=await gen.next();
  }
  expect(received).toEqual([...Array(8).fill(0x55),0x99]);
  expect(produced).toBe(2);
  expect(result.value.outputBytes).toBe(9);
});
