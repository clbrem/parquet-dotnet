namespace Parquet

open System
open System.IO

module Stream =
    let readInt32Async(stream: Stream) =
        async {
            let tmp = Array.create sizeof<int> 0uy
            let! _ = stream.ReadAsync(tmp, 0, sizeof<int>) |> Async.AwaitTask
            return BitConverter.ToInt32(tmp,0)
        }
    let readBytes i (stream: Stream) =
        async {
            let tmp = Array.create i 0uy
            let! _ = stream.ReadAsync(tmp, 0, i) |> Async.AwaitTask
            return tmp
        }

module File =
    let readFooterAsString(file: Stream) =
        async {
            do file.Seek (-8, SeekOrigin.End) |> ignore            
            let! footerSize = Stream.readInt32Async file
            do file.Seek (int64 (-8 - footerSize), SeekOrigin.End) |> ignore
            let! footerData = file |> Stream.readBytes footerSize
            use ms = new MemoryStream(footerData)
            return System.Text.Encoding.UTF8.GetString(footerData)
        }

