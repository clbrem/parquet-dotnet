namespace Parquet
open System

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
