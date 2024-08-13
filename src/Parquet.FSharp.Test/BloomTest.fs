namespace Parquet
open Xunit
open MyAssert
open System
module BloomTest =
    [<Fact>]
    let ``Can Write Block``() =        
        let rnd = Random()
        let salt = Salt.random rnd
        let data = Array.init 10 (fun _ -> Salt.rndUint32 rnd)                
        let block = Array.fold (Block.insert salt) Block.empty data
        for i in data do
            Block.check salt block i |> Assert.True
        ()
    [<Fact>]
    let ``Can CreateBloomFilter``() =
        let rnd = Random()
        let data = Array.init 1000 (fun _ -> Salt.rndUint64 rnd)
        let fakeData  = Array.init 500 (fun _ -> Salt.rndUint64 rnd)        
        let filter =
            Array.fold Filter.insert (Filter.create rnd 100) data        
        for datum in data do
            Filter.check filter datum |> Assert.True
        for fake in fakeData do
            Filter.check filter fake |> Assert.False

        // let block = Block.empty
        ()
        


