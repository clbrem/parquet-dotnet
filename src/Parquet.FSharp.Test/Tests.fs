module Tests

open System.IO
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
    
    
[<Fact>]
let ``StreamTricks`` () =    
    use s = streamFromText "hello pardner"
    match readChar s with
    | 'i', ReadChar ('e',  _) -> Assert.False true
    | 'h', ReadChar ('e', _) -> Assert.Fail $"Position is {s.Position}"
    | _ -> Assert.False true

        

    
    