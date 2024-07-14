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

    let stopwatch = System.Diagnostics.Stopwatch.StartNew()
    Parquet.fileMetadata state |> ignore
    stopwatch.Stop()    
    Assert.FailWith "Time is %i" stopwatch.ElapsedMilliseconds


    
            
            
            
    
        

    
    