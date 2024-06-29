namespace Parquet

open System
open System.IO

type CompactType =
    | Stop = 0x00
    | BooleanTrue = 0x01
    | BooleanFalse = 0x02
    | Byte = 0x03
    | I16 = 0x04
    | I32 = 0x05
    | I64 = 0x06
    | Double = 0x07
    | Binary = 0x08
    | List = 0x09
    | Set = 0x0A
    | Map = 0x0B
    | Struct = 0x0C
    | Uuid = 0x0D

type ThriftState = {
    stream: Stream
    stack: int16 list
    }
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
module ThriftCompact =
    let private beginStruct (state: ThriftState) =
        { state with
            stack = 0s :: state.stack
        }
    let private endStruct (state: ThriftState) =
        match state.stack with
        | _ :: rest -> { state with stack = rest }
        | _ -> InvalidOperationException("Malformed Thrift stream") |> raise
    
    let private zigZagToInt32 (n: uint) =
        int (n >>> 1) ^^^ (-(int n &&& 1))
    let private zigZagToInt64 (n: uint64) =
        int64 (n >>> 1) ^^^ (-(int64 n &&& 1))
    let private readVarInt32 (state: ThriftState) =
        let rec loop acc shift =            
            let b = state.stream.ReadByte() |> byte
            let acc = acc ||| (uint (b &&& 0x7fuy) <<< shift) 
            if b &&& 0x80uy <> 0x80uy then
                acc
            else
                loop acc (shift + 7)
        loop 0u 0, state
    let private readVarInt64 (state: ThriftState) =
        let rec loop acc shift =            
            let b = state.stream.ReadByte() |> byte
            let acc = acc ||| (uint64 (b &&& 0x7fuy) <<< shift) 
            if b &&& 0x80uy <> 0x80uy then
                acc
            else
                loop acc (shift + 7)
        loop 0UL 0, state
    let private readByte (state: ThriftState) =
        state.stream.ReadByte() |> sbyte, state
    
    let private readI16 (state: ThriftState) =
        let i, s = readVarInt32 state
        zigZagToInt32 i |> int16, s
    let private swapField state =
        let a,newState = readI16 state
        match newState.stack with
        | _ :: rest -> {newState  with stack = a :: rest}
        | _ -> {state with stack = [a]}
    let private fieldDelta b state =
        match state.stack with
        | a :: rest -> {state with stack = (a+b) :: rest}
        | _ -> {state with stack = [b]}        
    let private readI32 (state: ThriftState) =
        let i, s = (readVarInt32 state)
        zigZagToInt32 i, s
    
    let private readI64 (state: ThriftState) =
        let i, s = (readVarInt64 state)
        zigZagToInt64 i, s
    
    let private readBinary (state: ThriftState) =
        let uLen, s = readVarInt32 state
        let len = int uLen
        if (len = 0 ) then
            [||], s
        else
            Stream.readBytes len s.stream, s
   
    let private readString (state: ThriftState) =
        let uLen, s = readVarInt32 state
        let len = int uLen
        if (len = 0 ) then
            String.Empty, s
        else
            System.Text.Encoding.UTF8.GetString(Stream.readBytes len s.stream, 0, len)
            , s
    
    let private readListHeader (state: ThriftState) =
        let sizeAndType = state.stream.ReadByte()
        let size = sizeAndType >>> 4 &&& 0x0
        if size = 15 then
            let size, s = readVarInt32 state
            int size, s
        else
            int size, state
    let (|FieldId|) state =
        match state.stack with
        | a :: rest -> a, state
        | _ -> 0s, state
    let (|StructField|ExitStruct|) state =
        let header = state.stream.ReadByte()
        if enum header = CompactType.Stop then
            ExitStruct
        else
            let modifier = header &&& 0xf0 >>> 4 |> int16
            let compactType = header &&& 0x0f
            if modifier = 0s then
                StructField (compactType, swapField state)
            else
                StructField (compactType, fieldDelta modifier state)
                
            
                    
                    
            
                
            
                
        
        

        
          

module File =
    let readThriftAsync (file: Stream) =
        async {
            let! headerSize = Stream.readInt32Async file
            let! headerData = file |> Stream.readBytesAsync headerSize
            use ms = new MemoryStream(headerData)            
            return ()
        }
    let readFooterAsString(file: Stream) =
        async {
            do file.Seek (-8, SeekOrigin.End) |> ignore            
            let! footerSize = Stream.readInt32Async file
            do file.Seek (int64 (-8 - footerSize), SeekOrigin.End) |> ignore
            let! footerData = file |> Stream.readBytesAsync footerSize
            return ()
        }
