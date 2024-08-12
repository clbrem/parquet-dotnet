namespace Parquet
open Xunit
open MyAssert
open System
module BloomTest =
    [<Fact>]
    let ``Can Write Block``() =        
        let rnd = Random()
        let salt = Salt.random rnd
        let data = Array.init 1000 (fun _ -> Salt.rndUint32 rnd)        
        let block = Block.empty
        let block = Array.fold (Block.insert salt) block data
        Block.printBlock block |> Assert.FailWith "%s"            
        ()
        
        
        
        
        
        
        
        
        
        
        // let block = Block.empty
        ()
        


