namespace Parquet
open System
open System.IO
open System.Buffers

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
module Stream =
    let readBytes i (stream: Stream) =
        let rec loop read buffer =
            let r = stream.Read(buffer, read, i-read)
            if r = 0 then
                buffer
            else
                loop (read + r) buffer            
        let tmp = Array.create i 0uy
        loop 0 tmp

type ThriftState = {
    stream: Stream
    stack: int16 list
    }
module ThriftState =
    let create stream = { stream = stream; stack = [] }
module ThriftCompact =
    let mutable private varInt = Array.create 10 0uy
    let mutable private varIntCount = 0
    let enterStruct (state: ThriftState) =
        { state with
            stack = 0s :: state.stack
        }
    let exitStruct (state: ThriftState) =
        match state.stack with
        | _ :: rest -> { state with stack = rest }
        | _ -> {state with stack = []}
                
    let writeStopStruct (state: ThriftState) =
        do state.stream.WriteByte(byte CompactType.Stop)
        state
    let private int32ToVarInt (n: uint32) =
        let rec loop i m =
            if m &&& ~~~0x7fu = 0u then
                varInt[i] <- byte m
                varIntCount <- i + 1
                varInt, varIntCount
            else
                varInt[i] <- (m &&& 0x7fu) ||| 0x80u |> byte
                loop (i+1) (m >>> 7 )
        loop 0 n
    let private int64ToVarInt (n: uint64) =        
        let rec loop i m =
            if m &&& ~~~0x7FUL = 0UL then
                varInt[i] <- byte m
                varIntCount <- i + 1
                varInt
            else
                varInt[i] <- (m &&& 0x7FUL) ||| 0x80UL |> byte
                loop (i+1) (m >>> 7 )
        loop 0 n
        
    let private zigZagToInt32 (n: uint) =
        int (n >>> 1) ^^^ (-(int (n &&& 1u)))
    let private zigZagToInt64 (n: uint64) =
        int64 (n >>> 1) ^^^ (-(int64 (n &&& 1UL)))
    let private int64ToZigZag (n: int64) =
        uint64 (n <<< 1) ^^^ uint64 (n >>> 63)
    let private int32ToZigZag (n: int32) =
        uint (n <<< 1) ^^^ uint (n >>> 31)
    let private readVarInt32 (state: ThriftState) =
        let rec loop acc shift =            
            let b = state.stream.ReadByte() |> byte
            let acc = acc ||| (uint (b &&& 0x7fuy) <<< shift) 
            if b &&& 0x80uy <> 0x80uy then
                acc
            else
                loop acc (shift + 7)
        loop 0u 0, state        
    let private readI16 (state: ThriftState) =
        let i, s = readVarInt32 state
        zigZagToInt32 i |> int16, s        
    let private writeI16( n: int16)  (state: ThriftState) =
        let v, ct = int32ToVarInt (int32ToZigZag (int32 n))
        state.stream.Write (v, 0, ct)
        state    
    let private pushField (fieldId: int16) state =
        match state.stack with
        | _ :: rest -> {state with stack = fieldId :: rest}
        | _ -> {state with stack = [fieldId]}
    let private swapField state =
        let a,newState = readI16 state
        match newState.stack with
        | _ :: rest -> {newState  with stack = a :: rest}
        | _ -> {state with stack = [a]}
    let private fieldDelta b state =
        match state.stack with
        | a :: rest -> {state with stack = (a+b) :: rest}
        | _ -> {state with stack = [b]}
            
    let writeI32(n: int) (state: ThriftState) =
        let v, ct = int32ToVarInt (int32ToZigZag (int32 n))
        state.stream.Write (v, 0, ct)
        state
    let private writeI64(n: int64) (state: ThriftState)  =
        let v = int64ToVarInt (int64ToZigZag n)
        state.stream.Write (v, 0, varIntCount)
        state
    
    let private getFieldId state =
        match state.stack with
        | a :: _ -> a
        | _ -> 0s    
    let (|FieldId|) state =
        getFieldId state
    let private (|Delta|_|) (fieldId: int16, lastFieldId: int16): byte option =
        if fieldId > lastFieldId then
            let d = fieldId - lastFieldId 
            if d <= 15s then
                Some (byte d <<< 4)
            else
                None
        else
            None
        
    let private writeFieldBegin ( fieldId: int16, ct: CompactType) (state: ThriftState) =
        match (fieldId, getFieldId state) with
        | Delta d ->            
            let b = d ||| byte ct
            state.stream.WriteByte b
            state |> pushField fieldId
        | _ ->
            state.stream.WriteByte (byte ct)
            writeI16 fieldId state 
            |> pushField fieldId    
    let writeI16Field (fieldId, n) =
        writeFieldBegin (fieldId, CompactType.I16)
        >> writeI16 n
    let writeI32Field (fieldId, n) =
        writeFieldBegin (fieldId, CompactType.I32)
        >> writeI32 n
    let writeI64Field (fieldId, n) =
        writeFieldBegin (fieldId, CompactType.I64)
        >> writeI64 n
    let writeBinaryValue (value: byte[]) state=
        let n = Array.length value
        let newState = writeI32 n state
        newState.stream.Write(value, 0, n)
    let writeBinaryField (fieldId, value) =
        writeFieldBegin(fieldId, CompactType.Binary)
        >> writeBinaryValue value
    
    let writeStringValue (value: string) state=
        let buf = ArrayPool<byte>.Shared.Rent(System.Text.Encoding.UTF8.GetByteCount(value) )
        try 
            let numBytes = System.Text.Encoding.UTF8.GetBytes(value, 0, value.Length, buf, 0)
            let newState = writeI32 numBytes state
            newState.stream.Write(buf, 0, numBytes)
            newState
        finally 
            ArrayPool<byte>.Shared.Return(buf)
    let writeStringField (fieldId, value) =
        writeFieldBegin(fieldId, CompactType.Binary)
        >> writeStringValue value
    let writeListBegin (fieldId, elementType: CompactType, elementCount) state=
        let newState = writeFieldBegin(fieldId, CompactType.List) state
        if elementCount < 15 then
            newState.stream.WriteByte(byte (elementCount <<< 4) ||| byte elementType)
            newState
        else
            newState.stream.WriteByte(byte 0xf0 ||| byte elementType)
            writeI32 elementCount newState
            
    let private readVarInt64 (state: ThriftState) =
        let rec loop acc shift =            
            let b = state.stream.ReadByte() |> byte
            let acc = acc ||| (uint64 (b &&& 0x7fuy) <<< shift) 
            if b &&& 0x80uy <> 0x80uy then
                acc
            else
                loop acc (shift + 7)
        loop 0UL 0, state
    let readByte (state: ThriftState) =
        state.stream.ReadByte() |> sbyte, state
    
    let writeStartStruct fieldId  =
        writeFieldBegin(fieldId, CompactType.Struct) 
    
    let private writeBool (b: bool) (state: ThriftState) =
        if b then
            state.stream.WriteByte(byte CompactType.BooleanTrue)
        else
            state.stream.WriteByte(byte CompactType.BooleanFalse)
        state
    
    let private writeBoolField (fieldId, b) =
        if b then
            writeFieldBegin(fieldId, CompactType.BooleanTrue) 
        else
            writeFieldBegin(fieldId, CompactType.BooleanFalse)

    let private writeByteField (fieldId, b: sbyte) state =
        let newState = writeFieldBegin(fieldId, CompactType.Byte) state
        newState.stream.WriteByte(byte b)
        newState
            
    let readI32 (state: ThriftState) =
        let i, s = (readVarInt32 state)
        zigZagToInt32 i, s
    
    let readI64 (state: ThriftState) =
        let i, s = (readVarInt64 state)
        zigZagToInt64 i, s
    
    let readBinary (state: ThriftState) =
        let uLen, s = readVarInt32 state
        let len = int uLen
        if (len = 0 ) then
            [||], s
        else
            Stream.readBytes len s.stream, s
   
    let readString (state: ThriftState) =
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
    let (|List|) state =
        readListHeader state
    let (|Collect|) collector state =
        let (count,ct) , st = readListHeader state
        [1 .. count]
        |> List.fold
               (
                fun acc _ ->
                  let v, s = collector st
                  v :: (fst acc), s
               )        
               ([], st)
        
    let (|String|) state =
        readString state
    let (|I64|) state =
        readI64 state
    let (|I32|) state =
        readI32 state
    let (|I16|) = readI16
    let (|Binary|) state =
        readBinary state
    let readNextField state: CompactType * ThriftState =
        let header = state.stream.ReadByte()
        if enum header = CompactType.Stop then
            CompactType.Stop, state
        else
            let modifier = header &&& 0xf0 >>> 4 |> int16
            let compactType = header &&& 0x0f
            if modifier = 0s then
                enum compactType, swapField state
            else
                enum compactType, fieldDelta modifier state
    type ThriftCollection = | ThriftList of int * CompactType | ThriftStruct
    
    let skip (ct: CompactType) (st: ThriftState)  =
        let rec loop compactType (coll: ThriftCollection list) state =
            match coll, compactType with
            | [], None -> state
            | ThriftList (0, ct) :: rest, None ->
                loop None rest state
            | ThriftList (i, ct) :: rest, None ->
                loop (Some ct) ( ThriftList (i-1, ct) :: rest)  state
            | ThriftStruct :: rest, None ->
                let ct, st = readNextField state
                loop (Some ct) coll st
            | _, Some CompactType.BooleanTrue
            | _, Some CompactType.BooleanFalse ->
                state |> loop None coll  
            | _, Some CompactType.Byte ->
                state.stream.Seek(1, SeekOrigin.Current) |> ignore
                state |> loop None coll
            | _, Some CompactType.I16 -> 
               readI16 state |> snd |> loop None coll
            | _, Some CompactType.I32 ->
                readI32 state |> snd |> loop None coll
            | _, Some CompactType.I64 ->
                readI64 state |> snd |> loop None coll
            | _, Some CompactType.Binary ->
                readBinary state |> snd |> loop None coll
            | ThriftStruct :: rest, Some CompactType.Stop -> state |> exitStruct |> loop None rest
            | _, Some CompactType.Struct ->
                loop None  (ThriftStruct :: coll) state
            |_, Some CompactType.List ->
                let (ct, eltType), state = readListHeader state
                loop None (ThriftList(ct, eltType) :: coll) state                                      
            |_, Some compactType-> InvalidOperationException($"Can't skip type {compactType}.") |> raise
        loop (Some ct) [] st
    let (|EmptyStruct|) st =
        skip CompactType.Struct st

