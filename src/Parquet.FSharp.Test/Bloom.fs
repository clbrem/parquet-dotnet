namespace Parquet
open System
open System.Text

type Salt = private Salt of uint32 []
type SaltException() = inherit Exception("Salt can only be built from 8 odd uint32 values.")
type Block = uint32 []
module Salt =    
    let mul (Salt salt) a : Block=
        [| for s in salt do yield a * s |]
        
    let rndUint32 (rnd: Random) =
        let i = rnd.Next( 1 <<< 30) |> uint32
        let j = rnd.Next( 1 <<< 1) |> uint32
        (i <<< 2 ) ||| (j <<< 1) ||| 1u
    let rndUint64 (rnd: Random) =
        let i = rnd.NextInt64( 1 <<< 62) |> uint64
        let j = rnd.NextInt64( 1 <<< 1) |> uint64
        (i <<< 2 ) ||| (j <<< 1) ||| 1UL 
    let private checkLength : uint32 [] -> bool=
        Array.length >> (=) 8 
    let private checkOdd =
        Array.forall (fun x -> x &&& 1u = 1u)
    let ofArray =
        function
        | arr when checkLength arr && checkOdd arr -> Salt arr
        | _ -> raise (SaltException())
    let random (rnd: Random) =
        Array.init 8 (fun _ -> rndUint32 rnd) |> ofArray


module Block =
    let empty = Array.zeroCreate 8
    let private printWord =
        let rec loop i acc wd=
            if i = 32
            then
                acc.ToString()
            else
                acc +  (wd &&& 1u |> string)
                |> loop (i+1)
                <| (wd >>> 1)
        
        "" |> loop 0
    let printBlock: Block -> string =
        Array.map printWord >> String.concat Environment.NewLine
    let mask (salt: Salt) (x: uint32) : Block =
         Salt.mul salt x
         |> Array.map2 (
                 fun y ->
                    (|||) (1u <<< (int32 (y >>> 27) )) 
             ) 
         <| empty
         
    let insert (salt: Salt) (block: Block ) x : Block =
        mask salt x
        |> Array.map2 (|||) block

    let check (salt: Salt) (block: Block) x =
        mask salt x
        |> Array.map2 (&&&) block
        |> Array.contains 0u
        |> not
type SplitBlockBloomFilter = private SplitBlock of Salt * Block [] 
module Filter =
    let insert filter (x: uint64) =
        match filter with
        | SplitBlock (salt, blocks) ->
            let index = ((Array.length blocks|> uint64) * (x >>> 32)) >>> 32 |> int
            blocks[index] <- Block.insert salt blocks[index] (uint32 x) 
            SplitBlock (salt, blocks)
    let check filter (x: uint64)=         
        match filter with
        | SplitBlock (salt, blocks) ->
            let index = ((Array.length blocks|> uint64) * (x >>> 32)) >>> 32 |> int
            blocks[index]
            |> Block.check salt
            <| (uint32 x)
    let blocks = function | SplitBlock (_,blocks) -> Array.length blocks
    let salt = function | SplitBlock (salt, _) -> salt
    let empty n salt =
        SplitBlock (salt, Array.replicate n Block.empty)
    let create rnd n  =
        SplitBlock (Salt.random rnd, Array.replicate n Block.empty)
    
    let print =
        function
        | SplitBlock (_, blocks) ->
            Array.map Block.printBlock blocks
            |> String.concat ($"{Environment.NewLine}-------------------{Environment.NewLine}")
        
    