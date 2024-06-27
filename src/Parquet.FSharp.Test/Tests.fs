module Tests

open System.IO
open Xunit

[<Fact>]
let ``Can Read Footer`` () =
    async {
        use file = File.OpenRead("data/postcodes.plain.parquet")
        let! footer = Parquet.File.readFooterAsString file
        Assert.Contains("parquet-mr version 1.8.1", footer)
    }
    
    