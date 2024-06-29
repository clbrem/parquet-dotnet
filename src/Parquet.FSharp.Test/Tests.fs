module Tests

open System.IO
open Parquet
open Xunit


let readChar (stream: Stream) =
    let b = stream.ReadByte() |> byte
    char b, stream
let (|ReadChar|_|) (stream: Stream) =
    if stream.Length = stream.Position then
        None
    else
        Some (readChar stream)
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
    match readChar s with
    | 'i', ReadChar ('e',  _) -> Assert.False true
    | 'h', ReadChar ('e', _) -> Assert.Fail $"Position is {s.Position}"
    | _ -> Assert.Fail "Whoops!!"

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
}

[<Fact>]
let ``Can Read Thrift``() =
    let state = streamFromTestFile "thrift/wide.bin"
                |> ThriftState.create
                |> ThriftCompact.enterStruct    
    let rec loop  acc =
        function
        |  
            
    
        

    
    