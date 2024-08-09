namespace Parquet
open System
open System.IO.Hashing
type Salt = private Salt of uint32 []
type SaltException() = inherit Exception("Salt can only be built from 8 odd uint32 values.")
module Salt =

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
type Block = uint32 []
    

