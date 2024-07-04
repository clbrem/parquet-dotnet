module Tests

open System.IO
open Parquet
open Xunit

type Assert<'T>() =
    inherit Xunit.Assert() with
      static member FailWith f (a: 'T)=
           Assert.Fail(sprintf f a) 
      static member EqualTo (a:'T) (b:'T) =
          Assert.Equal<'T>(a,b)

type Mini = Stream * char list
let (|Pop|_|) =
    function
    | s, a::rest ->
        Some a
    | s, _ ->
        None

type ParquetType = 
    | BOOLEAN = 0
    | INT32 = 1
    | INT64 = 2
    | INT96 = 3
    | FLOAT = 4
    | DOUBLE = 5
    | BYTE_ARRAY = 6
    | FIXED_LEN_BYTE_ARRAY = 7
type RepetitionType =
    | REQUIRED = 0
    | OPTIONAL = 1
    | REPEATED = 2
type ConvertedType =
    | UTF8 = 0
    | MAP = 1
    | MAP_KEY_VALUE = 2
    | LIST = 3
    | ENUM = 4
    | DECIMAL = 5
    | DATE = 6
    | TIME_MILLIS = 7
    | TIME_MICROS = 8
    | TIMESTAMP_MILLIS = 9
    | TIMESTAMP_MICROS = 10
    | UINT_8 = 11
    | UINT_16 = 12
    | UINT_32 = 13
    | UINT_64 = 14
    | INT_8 = 15
    | INT_16 = 16
    | INT_32 = 17
    | INT_64 = 18
    | JSON = 19
    | BSON = 20
    | INTERVAL = 21
    
    
    
    
    
    
type SchemaElement = {
    typ: ParquetType option
    typLength: int option
    repetitionType: RepetitionType option
    name: string option
    numChildren: int option
    convertedType: 
}

let schemaElement (state: ThriftState) =
    ThriftCompact.enterStruct state    
    let loop acc state
    

let (|ReadChar|) =
    function
    | s: Stream, rest ->
        use reader = new StreamReader(s)
        let c = reader.Read() |> char
        s, c::rest

let streamFromText (s: string) =
    let str = new MemoryStream()
    let writer = new StreamWriter(str)
    writer.Write(s)
    writer.Flush()
    str.Position <- 0L
    str

let streamFromTestFile name =
    File.OpenRead($"./data/{name}")
        
[<Fact>]
let ``StreamTricks``() =    
    use s = streamFromText "hello pardner"
    let mini = s, []
    match mini with
    | Pop 'i' & ReadChar (_, 'h':: _) -> Assert.Fail "1"
    | Pop 'h' & ReadChar (_, 'e':: _) -> Assert.Fail "2"
    | Pop 'h' & ReadChar (_, 'l':: _ ) -> Assert.Fail "3"
    | ReadChar (_, 'h':: _ ) -> Assert.Fail "4"
    | _-> Assert.Fail "Whoops"

type SchemaElement = {
    parquetType: int option
    typeLength: int option
    repetitionType: int option
    name: string option
    numChildren: int option
    precision: int option
    fieldId: int option
    logicalType: int option
}
type FileMetaData = {
    version: int
    schema: SchemaElement list
    numRows: int64
    createdBy: string
} with
   static member Default() = { version= 0; schema= []; numRows = 0L; createdBy = ""}

[<Fact>]
let ``Can Write Thrift``() =
    use stream = new MemoryStream()
    let writeMiniStruct i =
        ThriftCompact.enterStruct        
        >> ThriftCompact.writeI32Field (2s, i)
        >> ThriftCompact.exitStruct
        >> ThriftCompact.writeStopStruct
    let state =
        ThriftState.create stream
        |> ThriftCompact.enterStruct
        |> ThriftCompact.writeListBegin (1s, CompactType.Struct, 4)
        |> writeMiniStruct 1
        |> writeMiniStruct 2
        |> writeMiniStruct 3
        |> writeMiniStruct 4
        |> ThriftCompact.writeStartStruct 2s
        |> ThriftCompact.writeI16Field (1s, 2s)
        |>ThriftCompact.exitStruct
        |> ThriftCompact.writeStopStruct
    state.stream.Flush()
    state.stream.Position <- 0
    state
    |> ThriftCompact.readNextField
    |> function
        | CompactType.Stop, _ -> failwith "No fields"
        | _, ThriftCompact.FieldId 1s & newState  ->
            ThriftCompact.skip CompactType.List newState
            |> ThriftCompact.readNextField
            |> function
                | CompactType.Stop, _ -> Assert.True true
                | cpt, _ -> failwith "There's more"
        | _, _ -> failwith "no list"    
    
        
            
        
        
    

[<Fact>]
let ``Can Read Thrift``() =
    let state = streamFromTestFile "thrift/wide.bin"
                |> ThriftState.create
                |> ThriftCompact.enterStruct    
    let rec loop acc =
        ThriftCompact.readNextField >>
        function
        | CompactType.Stop, _ ->
            acc, ThriftCompact.exitStruct state 
        | _, ThriftCompact.FieldId 1s        
             & ThriftCompact.I32 (version, state) ->
            loop { acc with version = version } state
        | cpt, ThriftCompact.FieldId 2s ->
            ThriftCompact.skip cpt state |> loop acc
        | _, ThriftCompact.FieldId 3s ->
            failwith "whoops"
        | _, ThriftCompact.FieldId 3s & ThriftCompact.I64 (numRows, state) ->
            loop {acc with numRows = numRows } state
        | cpt, ThriftCompact.FieldId 4s ->
            ThriftCompact.skip cpt state |> loop acc
        | cpt, ThriftCompact.FieldId 5s ->
            ThriftCompact.skip cpt state |> loop acc
        | _, ThriftCompact.FieldId 6s & ThriftCompact.String(createdBy, state) ->
            loop {acc with createdBy = createdBy } state
        | cpt, ThriftCompact.FieldId 7s ->
            ThriftCompact.skip cpt state |> loop acc
        | cpt, ThriftCompact.FieldId 8s ->
            ThriftCompact.skip cpt state |> loop acc
        | cpt, ThriftCompact.FieldId 9s ->
            ThriftCompact.skip cpt state |> loop acc            
        | compactType, state ->
            state
            |> ThriftCompact.skip compactType
            |> loop acc
    let out, st = loop (FileMetaData.Default()) state
    Assert.Fail $"{out}"
    
            
            
            
    
        

    
    