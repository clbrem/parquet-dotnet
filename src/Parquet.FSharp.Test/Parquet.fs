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
module ThriftState =
    let create stream = { stream = stream; stack = [] }
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
    let enterStruct (state: ThriftState) =
        { state with
            stack = 0s :: state.stack
        }
    let exitStruct (state: ThriftState) =
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
        let size = sizeAndType >>> 4 &&& 0x0f
        let eltType = sizeAndType &&& 0x0f |> enum<CompactType>
        if size = 15 then
            let size, s = readVarInt32 state
            (int size, eltType) , s
        else
            (int size, eltType), state
    let (|String|) state =
        readString state
    let (|I64|) state =
        readI64 state
    let (|I32|) state =
        readI32 state
    let (|FieldId|) state =
        match state.stack with
        | a :: _ -> a
        | _ -> 0s
    let readNextField state: CompactType * ThriftState =
        let header = state.stream.ReadByte()
        if enum header = CompactType.Stop then
            CompactType.Stop , state
        else
            let modifier = header &&& 0xf0 >>> 4 |> int16
            let compactType = header &&& 0x0f
            if modifier = 0s then
                enum compactType, swapField state
            else
                enum compactType, fieldDelta modifier state

    let rec skip (compactType: CompactType) (state: ThriftState)  =     
        match compactType with
        | CompactType.BooleanTrue
        | CompactType.BooleanFalse ->
            state 
        | CompactType.Byte ->
            state.stream.Seek(1, SeekOrigin.Current) |> ignore
            state 
        | CompactType.I16 -> 
           readI16 state |> snd
        | CompactType.I32 ->
            readI32 state |> snd
        | CompactType.I64 ->
            readI64 state |> snd
        | CompactType.Binary ->
            readBinary state |> snd
        | CompactType.Struct ->
            let rec loop state =
               match readNextField state with
               | CompactType.Stop, _ -> state |> exitStruct
               | _, state -> loop state             
            loop (enterStruct state)               
        | CompactType.List ->
            let (ct, eltType), state = readListHeader state
            [1..ct]
            |> List.fold (fun state _ -> skip eltType state) state                                        
        | compactType-> InvalidOperationException($"Can't skip type {compactType}.") |> raise
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

