namespace Parquet
open System
open System.Buffers
open System.IO

type Encoding =
    | PLAIN = 0
    | PLAIN_DICTIONARY = 2
    | RLE = 3
    | BIT_PACKED = 4
    | DELTA_BINARY_PACKED = 5
    | DELTA_LENGTH_BYTE_ARRAY = 6
    | DELTA_BYTE_ARRAY = 7
    | RLE_DICTIONARY = 8
    | BYTE_STREAM_SPLIT = 9
type CompressionCodec =
    | UNCOMPRESSED = 0
    | SNAPPY = 1
    | GZIP = 2
    | LZO = 3
    | BROTLI = 4
    | LZ4 = 5
    | ZSTD = 6
    | LZ4_RAW = 7
type ParquetType = 
    | BOOLEAN = 0
    | INT32 = 1
    | INT64 = 2
    | INT96 = 3
    | FLOAT = 4
    | DOUBLE = 5
    | BYTE_ARRAY = 6
    | FIXED_LEN_BYTE_ARRAY = 7
type RepetitionType =
    | REQUIRED = 0
    | OPTIONAL = 1
    | REPEATED = 2
type ConvertedType =
    | UTF8 = 0
    | MAP = 1
    | MAP_KEY_VALUE = 2
    | LIST = 3
    | ENUM = 4
    | DECIMAL = 5
    | DATE = 6
    | TIME_MILLIS = 7
    | TIME_MICROS = 8   
    | TIMESTAMP_MILLIS = 9
    | TIMESTAMP_MICROS = 10
    | UINT_8 = 11
    | UINT_16 = 12
    | UINT_32 = 13
    | UINT_64 = 14
    | INT_8 = 15
    | INT_16 = 16
    | INT_32 = 17
    | INT_64 = 18
    | JSON = 19
    | BSON = 20
    | INTERVAL = 21
type PageType  =
    | DATA_PAGE = 0
    | INDEX_PAGE = 1
    | DICTIONARY_PAGE = 2
    | DATA_PAGE_V2 = 3




module Parquet =
    [<Struct>]
    type KeyValue = {
        key: string
        value: string option
    }
    let keyValue (state: ThriftState) =
        let rec loop (acc: KeyValue) state =
            ThriftCompact.readNextField state
            |> function
               | CompactType.Stop, _ -> acc, ThriftCompact.exitStruct state
               | _, ThriftCompact.FieldId 1s & ThriftCompact.String (key, state) ->
                    loop { acc with key = key } state
               | _, ThriftCompact.FieldId 2s & ThriftCompact.String (value, state) ->
                    loop { acc with value = Some value } state
               | cpt, state ->
                    state            
                    |> ThriftCompact.skip cpt
                    |> loop acc
        state
        |> ThriftCompact.enterStruct
        |> loop { key = ""; value = None }
    [<Struct>]
    type SchemaElement = {
        parquetType: int32 option
        typeLength: int32 option
        repetitionType: int32 option
        name: string option
        numChildren: int32 option
        precision: int32 option
        fieldId: int32 option
        logicalType: int32 option
    } with static member Default: SchemaElement =
            {
            parquetType = None
            typeLength = None
            repetitionType = None
            name = None
            numChildren = None
            precision = None
            fieldId = None
            logicalType = None
            }
    [<Struct>]
    type Statistics = {
        max: byte [] 
        min: byte []
        nullCount: int64 option
        distinctCount: int64 option
        maxValue: byte[] option
        minValue: byte[] option
    } with static member Default: Statistics =
            {
                max = [||]
                min = [||]
                nullCount = None
                distinctCount = None
                maxValue = None
                minValue = None
            }
    let readStatistics =
        let rec loop acc state =
            match ThriftCompact.readNextField state with
            | CompactType.Stop, _ -> acc, ThriftCompact.exitStruct state
            | _, ThriftCompact.FieldId 1s & ThriftCompact.Binary (max, state) ->
                loop {acc with max = max} state
            | _, ThriftCompact.FieldId 2s & ThriftCompact.Binary (min, state) ->
                loop {acc with min = min} state
            | _, ThriftCompact.FieldId 3s & ThriftCompact.I64 (nullCount, state) ->
                loop {acc with nullCount = Some nullCount} state
            | _, ThriftCompact.FieldId 4s & ThriftCompact.I64 (distinctCount, state) ->
                loop {acc with distinctCount = Some distinctCount} state
            | _, ThriftCompact.FieldId 5s & ThriftCompact.Binary (maxValue, state) ->
                loop {acc with maxValue = Some maxValue} state
            | _, ThriftCompact.FieldId 6s & ThriftCompact.Binary (minValue, state) ->
                loop {acc with minValue = Some minValue} state
            | cpt, _ -> ThriftCompact.skip cpt state |> loop acc
        ThriftCompact.enterStruct >> loop Statistics.Default 
    let (|Statistics|) =
        readStatistics
    [<Struct>]
    type PageEncodingStats = {
        pageType: PageType
        encoding: Encoding
        count: int32    
    } with static member Default = {
            pageType = PageType.DATA_PAGE
            encoding = Encoding.PLAIN
            count = 0
        }
    let pageEncodingStats =
        let rec loop acc state =
            match ThriftCompact.readNextField state with
            | CompactType.Stop, _ -> acc, ThriftCompact.exitStruct state
            | _, ThriftCompact.FieldId 1s & ThriftCompact.I32 (pageType, state) ->
                loop {acc with pageType = enum pageType} state
            | _, ThriftCompact.FieldId 2s & ThriftCompact.I32 (encoding, state) ->
                loop {acc with encoding = enum encoding} state
            | _, ThriftCompact.FieldId 3s & ThriftCompact.I32 (count, state) ->
                loop {acc with count = count} state
            | cpt, state -> ThriftCompact.skip cpt state |> loop acc
        ThriftCompact.enterStruct >> loop PageEncodingStats.Default
    [<Struct>]
    type ColumnMetadata = {
        columnType: ParquetType
        encodings: Encoding list
        pathInSchema: string list
        codec: CompressionCodec
        numValues: int64
        totalUncompressedSize: int64
        totalCompressedSize: int64
        keyValueMetadata: KeyValue list
        dataPageOffset: int64
        indexPageOffset: int64 option
        dictionaryPageOffset: int64 option
        statistics: Statistics option
        encodingStats: PageEncodingStats list
        bloomFilterOffset: int64 option
        bloomFilterLength: int option    
    } with
       static member Default: ColumnMetadata  =
           {
               columnType = enum 0
               encodings = []
               pathInSchema = []
               codec = enum 0
               numValues = 0L
               totalUncompressedSize = 0L
               totalCompressedSize = 0L
               keyValueMetadata = []
               dataPageOffset = 0L
               indexPageOffset = None
               dictionaryPageOffset = None
               statistics = None
               encodingStats = []
               bloomFilterOffset = None
               bloomFilterLength = None
           }
        
    let columnMetadata =
        let rec loop acc st =
            match ThriftCompact.readNextField st with
            | CompactType.Stop, state -> acc, ThriftCompact.exitStruct state
            | _, ThriftCompact.FieldId 1s & ThriftCompact.I32 (columnType, state) ->
                loop { acc with columnType = enum columnType } state
            | _, ThriftCompact.FieldId 2s
                 & ThriftCompact.Collect
                     ThriftCompact.readI32 (items, newState) ->
                loop {acc with encodings = List.map enum items} newState
            | _, ThriftCompact.FieldId 3s
                 & ThriftCompact.Collect
                     ThriftCompact.readString (items, newState) ->
                loop {acc with pathInSchema = items} newState
            | _, ThriftCompact.FieldId 4s & ThriftCompact.I32 (codec, state) ->
                loop {acc with codec = enum codec} state
            | _, ThriftCompact.FieldId 5s & ThriftCompact.I64 (numValues, state) ->
                loop {acc with numValues = numValues} state
            | _, ThriftCompact.FieldId 6s & ThriftCompact.I64 (totalUncompressedSize, state) ->
                loop {acc with totalUncompressedSize = totalUncompressedSize} state
            | _, ThriftCompact.FieldId 7s & ThriftCompact.I64 (totalCompressedSize, state) ->
                loop {acc with totalCompressedSize = totalCompressedSize} state
            | _, ThriftCompact.FieldId 8s
                 & ThriftCompact.Collect
                     keyValue (items, newState) ->
                loop {acc with keyValueMetadata = items} newState
            | _, ThriftCompact.FieldId 9s & ThriftCompact.I64 (dataPageOffset, state) ->
                loop {acc with dataPageOffset = dataPageOffset} state
            | _, ThriftCompact.FieldId 10s & ThriftCompact.I64 (indexPageOffset, state) ->
                loop {acc with indexPageOffset = Some indexPageOffset} state
            | _, ThriftCompact.FieldId 11s & ThriftCompact.I64 (dictionaryPageOffset, state) ->
                loop {acc with dictionaryPageOffset = Some dictionaryPageOffset} state
            | _, ThriftCompact.FieldId 12s & Statistics (stats, state) ->  
                loop {acc with statistics = Some stats} state
            | _, ThriftCompact.FieldId 13s & ThriftCompact.Collect pageEncodingStats (items, state) ->
                loop { acc with encodingStats = items } state
            | _, ThriftCompact.FieldId 14s & ThriftCompact.I64 (bloomFilterOffset, state) ->
                loop { acc with bloomFilterOffset = Some bloomFilterOffset } state
            | _, ThriftCompact.FieldId 15s & ThriftCompact.I32 (bloomFilterLength, state) ->
                loop { acc with bloomFilterLength = Some bloomFilterLength } state
            | cpt, state -> ThriftCompact.skip cpt state |> loop acc
        ThriftCompact.enterStruct >> loop ColumnMetadata.Default
    let (|ColumnMetadata|) = columnMetadata
    [<Struct>]
    type EncryptionWithFooterKey =
        private EncryptionWithFooterKey of unit
        with static member Default = EncryptionWithFooterKey ()
    let encryptionWithFooterKey =
        let rec loop state =
            match ThriftCompact.readNextField state with         
            | CompactType.Stop, state -> EncryptionWithFooterKey.Default, ThriftCompact.exitStruct state
            | cpt, state -> ThriftCompact.skip cpt state |> loop
        ThriftCompact.enterStruct >> loop
    let (|EncryptionWithFooterKey|) = encryptionWithFooterKey
    [<Struct>]
    type EncryptionWithColumnKey =
        {
            pathInSchema : string list
            keyMetadata: byte[] option
        } with static member Default = {
                pathInSchema = []
                keyMetadata = None
            }
    let encryptionWithColumnKey =
        let rec loop (acc: EncryptionWithColumnKey) state =
            match ThriftCompact.readNextField state with
            | CompactType.Stop, st -> acc, ThriftCompact.exitStruct st
            | _, ThriftCompact.FieldId 1s & ThriftCompact.Collect ThriftCompact.readString (items, state) ->
                loop {acc with pathInSchema = items } state
            | _, ThriftCompact.FieldId 2s & ThriftCompact.Binary (keyMetadata, state) ->
                loop { acc with keyMetadata = Some keyMetadata } state
            | ct, state -> ThriftCompact.skip ct state |> loop acc
        ThriftCompact.enterStruct >> loop EncryptionWithColumnKey.Default
        
    let (|EncryptionWithColumnKey|) = encryptionWithColumnKey
    [<Struct>]
    type SortingColumn =
        {
            columnIdx : int
            descending: bool
            nullsFirst: bool
        } with
        static member Default = {
                columnIdx = 0
                descending = false
                nullsFirst = false
            }
    let sortingColumn =
        let rec loop acc state =
            match ThriftCompact.readNextField state with
            | CompactType.Stop, st -> acc, ThriftCompact.exitStruct st
            | _, ThriftCompact.FieldId 1s & ThriftCompact.I32 (columnIdx, state) ->
                loop { acc with columnIdx = columnIdx } state
            | CompactType.BooleanTrue, ThriftCompact.FieldId 2s & state ->
                loop { acc with descending = true} state
            | CompactType.BooleanFalse, ThriftCompact.FieldId 2s & state ->
                loop { acc with descending = false} state
            | CompactType.BooleanTrue, ThriftCompact.FieldId 3s & state ->
                loop { acc with nullsFirst =  true} state
            | CompactType.BooleanFalse, ThriftCompact.FieldId 3s & state ->
                loop { acc with nullsFirst =  false} state
            | cpt, state -> ThriftCompact.skip cpt state |> loop acc
        ThriftCompact.enterStruct >> loop SortingColumn.Default
    [<Struct>]
    type ColumnCryptoMetadata = {
        encryptionWithFooterKey: EncryptionWithFooterKey option
        encryptionWithColumnKey: EncryptionWithColumnKey option
    } with static member Default = {
            encryptionWithFooterKey = None
            encryptionWithColumnKey = None
        }
    let columnCryptoMetadata =
        let rec loop acc state =
            match ThriftCompact.readNextField state with
            | CompactType.Stop, _ -> acc, ThriftCompact.exitStruct state
            | _, ThriftCompact.FieldId 1s &  EncryptionWithFooterKey (enc, state) ->
                loop { acc with encryptionWithFooterKey = Some enc } state
            | _, ThriftCompact.FieldId 2s & EncryptionWithColumnKey (enc, state) ->
                loop { acc with encryptionWithColumnKey = Some enc } state
            | cpt, state -> ThriftCompact.skip cpt state |> loop acc
        ThriftCompact.enterStruct >> loop ColumnCryptoMetadata.Default
        
    let (|ColumnCryptoMetadata|) = columnCryptoMetadata
    [<Struct>]
    type ColumnChunk =
        {
            filePath: string option
            fileOffset: int64 option
            metaData: ColumnMetadata option
            offsetIndexOffset: int64 option
            offsetIndexLength: int32 option
            columnIndexOffset: int64 option
            columnIndexLength: int32 option
            cryptoMetadata: ColumnCryptoMetadata option
            encryptedColumnMetadata: byte []
        }
        with static member Default = {
                filePath = None
                fileOffset = None
                metaData = None
                offsetIndexOffset = None
                offsetIndexLength = None
                columnIndexOffset = None
                columnIndexLength = None
                cryptoMetadata = None
                encryptedColumnMetadata = [||]
            }
    let columnChunk =
        let rec loop acc state =
            match ThriftCompact.readNextField state with
            | CompactType.Stop, _ -> acc, ThriftCompact.exitStruct state
            | _, ThriftCompact.FieldId 1s & ThriftCompact.String (filePath, state) ->
                loop { acc with filePath = Some filePath } state
            | _, ThriftCompact.FieldId 2s & ThriftCompact.I64 (fileOffset, state) ->
                loop { acc with fileOffset = Some fileOffset } state
            | _, ThriftCompact.FieldId 3s & ColumnMetadata (metaData, state) ->
                loop { acc with metaData = Some metaData } state
            | _, ThriftCompact.FieldId 4s & ThriftCompact.I64 (offsetIndexOffset, state) ->
                loop { acc with offsetIndexOffset = Some offsetIndexOffset } state
            | _, ThriftCompact.FieldId 5s & ThriftCompact.I32 (offsetIndexLength, state) ->
                loop { acc with offsetIndexLength = Some offsetIndexLength } state
            | _, ThriftCompact.FieldId 6s & ThriftCompact.I64 (columnIndexOffset, state) ->
                loop { acc with columnIndexOffset = Some columnIndexOffset } state
            | _, ThriftCompact.FieldId 7s & ThriftCompact.I32 (columnIndexLength, state) ->
                loop { acc with columnIndexLength = Some columnIndexLength } state
            | _, ThriftCompact.FieldId 8s & ColumnCryptoMetadata (items, state) ->
                loop { acc with cryptoMetadata = Some items } state
            | _, ThriftCompact.FieldId 9s & ThriftCompact.Binary (encryptedColumnMetadata, state) ->
                loop { acc with encryptedColumnMetadata = encryptedColumnMetadata } state
            | cpt, state -> ThriftCompact.skip cpt state |> loop acc
        ThriftCompact.enterStruct >> loop ColumnChunk.Default
    [<Struct>]
    type ColumnOrder = |TypeDefinedOrder | Unordered
    [<Struct>]
    type AesGcmV1 = {
        aadPrefix: byte [] option
        aadFileUnique: byte [] option
        supplyAadPrefix: bool option
    } with static member Default = {
            aadPrefix = None
            aadFileUnique = None
            supplyAadPrefix = None
            }
    let aesGcmV1 =
        let rec loop acc =
            ThriftCompact.readNextField >>
            function
            | CompactType.Stop, st -> acc, ThriftCompact.exitStruct st
            | _, ThriftCompact.FieldId 1s & ThriftCompact.Binary (aadPrefix, state) ->
                loop { acc with aadPrefix = Some aadPrefix } state
            | _, ThriftCompact.FieldId 2s & ThriftCompact.Binary (aadFileUnique, state) ->
                loop { acc with aadFileUnique = Some aadFileUnique } state
            | CompactType.BooleanTrue, ThriftCompact.FieldId 3s & state ->
                loop { acc with supplyAadPrefix = Some true } state
            | CompactType.BooleanFalse, ThriftCompact.FieldId 3s & state ->
                loop { acc with supplyAadPrefix = Some true } state
            | ct, st -> ThriftCompact.skip ct st |> loop acc
        ThriftCompact.enterStruct >> loop AesGcmV1.Default
    let (|AesGcm|) = aesGcmV1
    [<Struct>]
    type EncryptionAlgorithm = | AesGcmV1 of aesGcmV1: AesGcmV1 | AesGcmCtrV1 of aesGcmCtrV1: AesGcmV1 | NoEncryption
    
    
    let encryptionAlgorithm =
        let rec loop acc =
            ThriftCompact.readNextField >>
            function
            | CompactType.Stop, st -> acc, ThriftCompact.exitStruct st
            | _, ThriftCompact.FieldId 1s & AesGcm (algo, state) ->
                loop (AesGcmV1 algo) state
            | _, ThriftCompact.FieldId 2s & AesGcm (algo, state) ->
                loop (AesGcmCtrV1 algo) state
            | cpt, st -> ThriftCompact.skip cpt st |> loop acc
        ThriftCompact.enterStruct >> loop NoEncryption
    let (|Encryption|) = encryptionAlgorithm
    let columnOrder =
        let rec loop acc state =
            match ThriftCompact.readNextField state with
            | CompactType.Stop, st -> acc, ThriftCompact.exitStruct st
            | _, ThriftCompact.FieldId 1s & ThriftCompact.EmptyStruct st ->
                loop TypeDefinedOrder st
            | ct, st -> ThriftCompact.skip ct st |> loop acc
        ThriftCompact.enterStruct >> loop ColumnOrder.Unordered
    [<Struct>]
    type RowGroup =
        {
            columns: ColumnChunk list
            totalByteSize: int64
            numRows: int64
            sortingColumns: SortingColumn list
            fileOffset: int64 option
            totalCompressedSize: int64 option
            ordinal: int16 option
            
        } with
        static member Default = {
                columns = []
                totalByteSize = 0L
                numRows = 0L
                sortingColumns = []
                fileOffset = None
                totalCompressedSize = None
                ordinal = None
            }
    
    let rowGroup =
        let rec loop acc state =
            match ThriftCompact.readNextField state with
            | CompactType.Stop, st -> acc, ThriftCompact.exitStruct st
            | _, ThriftCompact.FieldId 1s & ThriftCompact.Collect columnChunk (items, state) ->
                loop { acc with columns = items } state
            | _, ThriftCompact.FieldId 2s & ThriftCompact.I64 (totalByteSize, state) ->
                loop { acc with totalByteSize = totalByteSize } state
            | _, ThriftCompact.FieldId 3s & ThriftCompact.I64 (numRows, state) ->
                loop { acc with numRows = numRows } state
            | _, ThriftCompact.FieldId 4s & ThriftCompact.Collect sortingColumn (items, state) ->
                loop { acc with sortingColumns = items } state
            | _, ThriftCompact.FieldId 5s & ThriftCompact.I64 (fileOffset, state) ->
                loop { acc with fileOffset = Some fileOffset } state
            | _, ThriftCompact.FieldId 6s & ThriftCompact.I64 (totalCompressedSize, state) ->
                loop { acc with totalCompressedSize = Some totalCompressedSize } state
            | _, ThriftCompact.FieldId 7s & ThriftCompact.I16 (ordinal, state) ->
                loop { acc with ordinal = Some ordinal } state
            | cpt, state -> ThriftCompact.skip cpt state |> loop acc
        ThriftCompact.enterStruct >> loop RowGroup.Default
    
    
    
    let schemaElement (state: ThriftState) =        
        let rec loop (acc: SchemaElement) state =       
            ThriftCompact.readNextField state
            |> function
            | CompactType.Stop, _ -> acc, ThriftCompact.exitStruct state
            | _, ThriftCompact.FieldId 1s & ThriftCompact.I32 (parquetType, state) ->
                loop { acc with parquetType = Some parquetType } state
            | _, ThriftCompact.FieldId 2s & ThriftCompact.I32 (typeLength, state) ->
                loop { acc with typeLength = Some typeLength } state
            | _, ThriftCompact.FieldId 3s & ThriftCompact.I32 (repetitionType, state) ->
                loop { acc with repetitionType = Some repetitionType } state
            | _, ThriftCompact.FieldId 4s & ThriftCompact.String (name, state) ->
                loop { acc with name = Some name } state
            | _, ThriftCompact.FieldId 5s & ThriftCompact.I32 (numChildren, state) ->
                loop { acc with numChildren = Some numChildren } state
            | _, ThriftCompact.FieldId 6s & ThriftCompact.I32 (precision, state) ->
                loop { acc with precision = Some precision } state
            | _, ThriftCompact.FieldId 7s & ThriftCompact.I32 (fieldId, state) ->
                loop { acc with fieldId = Some fieldId } state
            | _, ThriftCompact.FieldId 8s & ThriftCompact.I32 (logicalType, state) ->
                loop { acc with logicalType = Some logicalType } state
            | cpt, state ->
                state
                |> ThriftCompact.skip cpt
                |> loop acc
        state
         |> ThriftCompact.enterStruct
         |> loop SchemaElement.Default     
         
          
        
    
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
    [<Struct>]
    type FileMetadata = {
        version: int
        schema: SchemaElement list
        numRows: int64
        rowGroups: RowGroup list    
        keyValueMetadata: KeyValue list option
        createdBy: string option
        columnOrders: ColumnOrder list option
        encryptionAlgorithm: EncryptionAlgorithm option
        footerSigningKeyMetadata : byte[] option
        
    } with
       static member Default() =
           {
               version= 0
               schema= []
               numRows = 0L
               rowGroups = []           
               createdBy = None
               keyValueMetadata = None
               columnOrders = None
               encryptionAlgorithm = None
               footerSigningKeyMetadata = None
           }
    let fileMetadata =
        let rec loop acc =
            ThriftCompact.readNextField >>
            function
            | CompactType.Stop, state ->
                acc, ThriftCompact.exitStruct state 
            | _, ThriftCompact.FieldId 1s        
                 & ThriftCompact.I32 (version, state) ->
                loop { acc with version = version } state
            | _, ThriftCompact.FieldId 2s
                 & ThriftCompact.Collect schemaElement (items, state)  ->
                loop {acc with schema = items} state
            | _, ThriftCompact.FieldId 3s & ThriftCompact.I64 (numRows, state) ->
                loop {acc with numRows = numRows } state        
            | _, ThriftCompact.FieldId 4s & ThriftCompact.Collect rowGroup (items, state)->
                loop { acc with rowGroups = items } state
            | _, ThriftCompact.FieldId 5s & ThriftCompact.Collect keyValue (items, state) ->
                loop { acc with keyValueMetadata = Some items } state 
            | _, ThriftCompact.FieldId 6s & ThriftCompact.String(createdBy, state) ->
                loop {acc with createdBy = Some createdBy } state
            | _, ThriftCompact.FieldId 7s & ThriftCompact.Collect columnOrder (cols, state) ->
                loop {acc with columnOrders = Some cols} state
            | _, ThriftCompact.FieldId 8s & Encryption (alg, state) ->
                loop { acc with encryptionAlgorithm = Some alg } state 
            | _, ThriftCompact.FieldId 9s & ThriftCompact.Binary (footerSigningKeyMetadata, state) ->
                loop {acc with footerSigningKeyMetadata = Some footerSigningKeyMetadata } state        
            | compactType, state ->
                state
                |> ThriftCompact.skip compactType
                |> loop acc
        ThriftCompact.enterStruct >> loop (FileMetadata.Default())
module File = 
    let readInt32Async(stream: Stream) =
        async {
            let tmp = Array.create sizeof<int> 0uy
            let! _ = stream.ReadAsync(tmp, 0, sizeof<int>) |> Async.AwaitTask
            return BitConverter.ToInt32(tmp,0)
        }

    let readBytesAsync i (stream: Stream) =
        async {
            let tmp = Array.create i 0uy
            let! _ = stream.ReadAsync(tmp, 0, i) |> Async.AwaitTask
            return tmp
        }    
    let readThriftAsync (file: Stream) =
        async {
            let! headerSize = readInt32Async file
            let! headerData = file |> readBytesAsync headerSize
            use ms = new MemoryStream(headerData)            
            return ()
        }
    let readFooterAsString(file: Stream) =
        async {
            do file.Seek (-8, SeekOrigin.End) |> ignore            
            let! footerSize = readInt32Async file
            do file.Seek (int64 (-8 - footerSize), SeekOrigin.End) |> ignore
            let! footerData = file |> readBytesAsync footerSize
            return ()
        }
    let readMetadata =
        function
        | ThriftState.Seek (-8, SeekOrigin.End) state->

            
        

