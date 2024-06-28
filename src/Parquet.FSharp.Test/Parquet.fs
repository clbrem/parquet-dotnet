namespace Parquet

open System
open System.IO

module Stream =
    let readInt32Async(stream: Stream) =
        async {
            let tmp = Array.create sizeof<int> 0uy
            let! _ = stream.ReadAsync(tmp, 0, sizeof<int>) |> Async.AwaitTask
            return BitConverter.ToInt32(tmp,0)
        }
    let readBytes i (stream: Stream) =
        let rec loop read buffer =
            let r = stream.Read(buffer, read, i-read)
            if r = 0 then
                buffer
            else
                loop (read + r) buffer            
        let tmp = Array.create i 0uy
        loop 0 tmp

    let readBytesAsync i (stream: Stream) =
        async {
            let tmp = Array.create i 0uy
            let! _ = stream.ReadAsync(tmp, 0, i) |> Async.AwaitTask
            return tmp
        }
module Thrift =
    let private zigZagToInt32 (n: uint) =
        int (n >>> 1) ^^^ (-(int n &&& 1))
    let private zigZagToInt64 (n: uint64) =
        int64 (n >>> 1) ^^^ (-(int64 n &&& 1))
    let private readVarInt32 (stream: Stream) =
        let rec loop acc shift =            
            let b = stream.ReadByte() |> byte
            let acc = acc ||| (uint (b &&& 0x7fuy) <<< shift) 
            if b &&& 0x80uy <> 0x80uy then
                acc
            else
                loop acc (shift + 7)
        loop 0u 0, stream
    let private readVarInt64 (stream: Stream) =
        let rec loop acc shift =            
            let b = stream.ReadByte() |> byte
            let acc = acc ||| (uint64 (b &&& 0x7fuy) <<< shift) 
            if b &&& 0x80uy <> 0x80uy then
                acc
            else
                loop acc (shift + 7)
        loop 0UL 0, stream
    let private readByte (stream: Stream) =
        stream.ReadByte() |> sbyte, stream
        
    let private readI16 (stream: Stream) =
        let i, s = readVarInt32 stream
        zigZagToInt32 i |> int16, stream
    
    let private readI32 (stream: Stream) =
        let i, s = (readVarInt32 stream)
        zigZagToInt32 i, s
    
    let private readI64 (stream: Stream) =
        let i, s = (readVarInt64 stream)
        zigZagToInt64 i, s
    
    let private readBinary (stream: Stream) =
        let uLen, s = readVarInt32 stream
        let len = int uLen
        if (len = 0 ) then
            [||], s
        else
            Stream.readBytes len s, s
   
    let private readString (stream: Stream) =
        let uLen, s = readVarInt32 stream
        let len = int uLen
        if (len = 0 ) then
            String.Empty, s
        else
            System.Text.Encoding.UTF8.GetString(Stream.readBytes len s, 0, len)
            , s        
    
module File =
    let readThriftAsync (file: Stream) =
        async {
            let! headerSize = Stream.readInt32Async file
            let! headerData = file |> Stream.readBytesAsync headerSize
            use ms = new MemoryStream(headerData)
            let! thrift = Thrift.readThriftAsync ms
            return thrift
        }
    let readFooterAsString(file: Stream) =
        async {
            do file.Seek (-8, SeekOrigin.End) |> ignore            
            let! footerSize = Stream.readInt32Async file
            do file.Seek (int64 (-8 - footerSize), SeekOrigin.End) |> ignore
            let! footerData = file |> Stream.readBytesAsync footerSize
            use ms = new MemoryStream(footerData)
        }

