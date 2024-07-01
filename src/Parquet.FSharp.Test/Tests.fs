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
    let state =
        ThriftState.create stream
        |> ThriftCompact.enterStruct
        |> ThriftCompact.writeListBegin (1s, CompactType.I32, 4)
        |> ThriftCompact.writeI32 1
        |> ThriftCompact.writeI32 2
        |> ThriftCompact.writeI32 3
        |> ThriftCompact.writeI32 4
        |> ThriftCompact.exitStruct
        |> ThriftCompact.writeStop
    state.stream.Flush()
    state.stream.Position <- 0L
    
    state
    |> ThriftCompact.enterStruct
    |> ThriftCompact.readNextField
    |> function
        | CompactType.Stop, _ -> failwith "No fields"
        | _, ThriftCompact.FieldId 1s & ThriftCompact.List((nItems, ct), newState) ->
            [1..nItems]
            |> List.fold (
                function
                | items, ThriftCompact.I32 (i, state) ->
                    fun _ -> (i :: items), state                
            ) ([], newState) 
            |> fst
            |> List.rev
            |> Assert.EqualTo [1;2;3;4]
        | _, _ -> failwith "no list"    
    
            
            
        
        
    

//[<Fact>]
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
    
            
            
            
    
        

    
    