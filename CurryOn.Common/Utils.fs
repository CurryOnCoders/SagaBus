namespace CurryOn.Common

open FSharp.Reflection
open System
open System.Collections
open System.IO
open System.Linq
open System.Runtime.Caching
open System.Security.Cryptography
open System.Text
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module Helpers =
    let inline func (f: 'a -> 'b) = new Func<'a,'b>(f)
    let inline isNull (x: 't when 't: null) = obj.ReferenceEquals (x, null)
    let inline isNotNull (x: 't when 't: null) = not (obj.ReferenceEquals (x, null))
    let inline ifNotNull a f = if a |> isNotNull then f a

    let valueOrDefault optionalParameter defaultValueGenerator =
       match optionalParameter with
       | Some value -> value
       | None -> defaultValueGenerator()

    let parseUnion<'a> (str: string) =
        let union = FSharpType.GetUnionCases typeof<'a> |> Array.find (fun case -> case.Name = str)
        FSharpValue.MakeUnion(union, [||]) :?> 'a

    let tryParseUnion<'a> (str: string) =
        let unionOpt = FSharpType.GetUnionCases typeof<'a> |> Array.tryFind (fun case -> case.Name = str)
        match unionOpt with
        | Some union -> FSharpValue.MakeUnion(union, [||]) :?> 'a |> Some
        | _ -> None

    let getUnionCase<'a> (value: 'a) =
        let union, _ = FSharpValue.GetUnionFields(value, typeof<'a>)
        union

    let isSameUnionCase<'a> (value: 'a) (unionCase: 'a) =
        let (leftCase, _) = FSharpValue.GetUnionFields(value, typeof<'a>)
        let (rightCase, _) = FSharpValue.GetUnionFields(unionCase, typeof<'a>)
        leftCase.Name = rightCase.Name

    let toEnum<'a> (union: UnionCaseInfo) =
        Enum.GetNames(typeof<'a>)
        |> Seq.find (fun name -> name = union.Name)
        |> (fun name -> Enum.Parse(typeof<'a>, name) :?> 'a)

    let stringToList (delimiter:char) (input:string) =
        input.Split([|delimiter|])
        |> Array.map(fun s -> s.Trim())
        |> List.ofArray

    // Operators
    let inline (?=) maybeNull defaultOption =
        if isNull maybeNull then defaultOption() else maybeNull

    let inline (!) (lazyValue: Lazy<'a>) = lazyValue.Force()

    let inline ret anyInput anyValue = anyInput |> (fun _ -> anyValue)

    let inline (|>|) anyInput anyValue = ret anyInput anyValue 

    let inline (?==) (nulVal1: Nullable<_>) (nulVal2: Nullable<_>) =
        nulVal1.HasValue && nulVal2.HasValue && nulVal1.Value = nulVal2.Value

    let inline (==) (str1: string) (str2: string) = 
        str1 |> String.IsNullOrWhiteSpace |> not && 
        str2 |> String.IsNullOrWhiteSpace |> not && 
        str1.Equals(str2, StringComparison.InvariantCultureIgnoreCase) 
    
    let inline (!=) (str1: string) (str2: string) = str1 == str2 |> not

    let inline like (pattern: string) (str: string)  = // Example Usage:  "F# is Cool" |> like "f#*cool" 
        if   str |> isNull then false
        elif pattern |> isNull then false
        elif pattern.StartsWith("/") then
            let regex = new Regex(pattern.Replace("/", ""), RegexOptions.IgnoreCase)
            regex.IsMatch(str)
        elif pattern.Contains("*") || pattern.Contains("%") then
            let pieces = pattern.Split ([|'*'; '%'|], StringSplitOptions.RemoveEmptyEntries)
            if   pieces.Length = 0 then true
            elif pieces.Length = 1 then 
                if pattern.StartsWith("*") || pattern.StartsWith("%") then 
                    if pattern.EndsWith("*") || pattern.EndsWith("%") then str.IndexOf(pieces.[0], StringComparison.InvariantCultureIgnoreCase) >= 0
                    else str.EndsWith(pieces.[0], StringComparison.InvariantCultureIgnoreCase)
                else str.StartsWith(pieces.[0], StringComparison.InvariantCultureIgnoreCase)
            else (pieces |> Seq.fold (fun pos key -> if pos = -1 then -1 else 
                                                        let index = str.IndexOf(key, pos, StringComparison.InvariantCultureIgnoreCase)
                                                        if index < 0 then index
                                                        else index + key.Length) 0) >= 0
        else str.Trim().Equals(pattern.Trim(), StringComparison.InvariantCultureIgnoreCase)

    let inline notLike pattern = like pattern >> not

    let inline isNullOrEmpty str = String.IsNullOrWhiteSpace str

    let inline isNotNullAndNotEmpty str = str |> (isNullOrEmpty >> not)

    let inline toLower str = if str |> isNotNullAndNotEmpty then str.ToLower() else str

    let (|Enumerable|_|) (any: obj) =
        if any |> isNull then None else
            let objType = any.GetType()
            if objType.GetInterfaces() |> Seq.contains typeof<IEnumerable>
            then Some (any :?> IEnumerable)
            else None

    let getTypeName any = any.GetType().Name
    let getFullTypeName any = any.GetType().FullName
    let getQualifiedTypeName any = any.GetType().AssemblyQualifiedName

    let mutate func ob = 
        func ob
        ob

    let defaultOf<[<Measure>]'m, 't, 'mt> (ctor: ('t -> 'mt)) = Unchecked.defaultof<'t> |> ctor


module Caching =
     type ICache<'a,'b> =
        abstract member has: 'a -> bool
        abstract member get: 'a -> 'b
        abstract member add: 'a -> 'b -> unit
        abstract member getOrAdd: 'a -> ('a -> 'b) -> 'b

     type Cache<'a,'b>(cacheName) = 
        let cache = new MemoryCache(cacheName)
        let getSafeKey key =
            if key |> box |> isNull
            then String.Empty
            else key.ToString()
        member c.has key = key |> getSafeKey |> cache.Contains
        member c.get key = cache.[getSafeKey key] |> unbox<'b>
        member c.add key value = cache.Add(getSafeKey key, value, DateTimeOffset.Now.AddDays(1.0)) |> ignore // TODO: Make the absolute expiration date configurable
        member c.getOrAdd key func =
            let safeKey = getSafeKey key
            if c.has safeKey 
            then c.get safeKey
            else let value = key |> func
                 c.add safeKey value
                 value                
        interface ICache<'a,'b> with
            member c.has key = c.has key
            member c.get key = c.get key
            member c.add key value = c.add key value 
            member c.getOrAdd key func = c.getOrAdd key func
        interface IDisposable with
            member c.Dispose () = cache.Dispose()    

[<AutoOpen>]
module Memoization =
    open Caching 

    [<Literal>]
    let MemoizationCacheName = "Memoization"

    let memoize f =
        let cache = new Cache<_,_>(MemoizationCacheName)
        (fun key -> f |> cache.getOrAdd key)

module Encoding =
    let ASCII = ASCIIEncoding()
    let UTF8NoBOM = UTF8Encoding(false)
    let UTF16LE = UnicodeEncoding(false, false)
    let UTF32 = UTF32Encoding()

    type EncodingStandard =
    | Ascii
    | Utf8
    | Utf16
    | Utf32

    let encode = function
    | Ascii -> ASCII.GetString
    | Utf8 -> UTF8NoBOM.GetString
    | Utf16 -> UTF16LE.GetString
    | Utf32 -> UTF32.GetString

    let decode: EncodingStandard -> string -> byte[] = 
        function
        | Ascii -> ASCII.GetBytes
        | Utf8 -> UTF8NoBOM.GetBytes
        | Utf16 -> UTF16LE.GetBytes
        | Utf32 -> UTF32.GetBytes

module Security =
    open Encoding
    
    module Hashing =
        let private Sha1 = SHA1.Create()
        let private Sha256 = SHA256.Create()
        let private Sha512 = SHA512.Create()
        let private Md5 = MD5.Create()

        type HashAlgorithm =
        | SHA1
        | SHA256
        | SHA512
        | MD5

        let hashBytes: HashAlgorithm -> byte[] -> byte[] =
            function
            | SHA1 -> Sha1.ComputeHash
            | SHA256 -> Sha256.ComputeHash
            | SHA512 -> Sha512.ComputeHash
            | MD5 -> Md5.ComputeHash

        let hash algorithm = decode Utf8 >> hashBytes algorithm >> Convert.ToBase64String
            
        let sha1 = hash SHA1
        let sha256 = hash SHA256
        let sha512 = hash SHA512
        let md5 = hash MD5

    module Encryption =
        let private initializationVector = [|107uy; 101uy; 101uy; 112uy; 99uy; 97uy; 108uy; 109uy; 38uy; 99uy; 117uy; 114uy; 114uy; 121uy; 111uy; 110uy|]
        let private zero _ = 0uy
        let private rng = RNGCryptoServiceProvider.Create()        

        type EncryptionAlgorithm =
        | AES of int*string
        | RSA of int*RSAEncryptionPadding

        let private aesCryptoTransform keySize key transform =
            let aes = AesCryptoServiceProvider.Create()
            let keyBytes = zero |> Array.init (keySize/8)                  
            let utf8Bytes = key |> decode Utf8
            Array.Copy(utf8Bytes, keyBytes, utf8Bytes.Length)                    
            aes.Mode <- CipherMode.CBC;
            aes.Padding <- PaddingMode.PKCS7;
            aes.IV <- initializationVector
            transform aes keyBytes           

        let encryptBytes algorithm bytes =
            match algorithm with
            | AES (keySize,key) -> 
                let encryptor = (fun (aes: Aes) keyBytes -> aes.CreateEncryptor(keyBytes, initializationVector)) |> aesCryptoTransform keySize key
                use stream = new MemoryStream()
                use cryptoStream = new CryptoStream(stream, encryptor, CryptoStreamMode.Write)            
                cryptoStream.Write(bytes, 0, bytes.Length)
                cryptoStream.FlushFinalBlock()
                stream.ToArray()
            | RSA (keySize,padding) ->
                let rsa = RSACng.Create()
                rsa.KeySize <- keySize
                rsa.Encrypt(bytes, padding)

        let decryptBytes algorithm (bytes: byte[]) =
            match algorithm with
            | AES (keySize,key) -> 
                let decryptor = (fun (aes: Aes) keyBytes -> aes.CreateDecryptor(keyBytes, initializationVector)) |> aesCryptoTransform keySize key
                use stream = new MemoryStream(bytes)
                use cryptoStream = new CryptoStream(stream, decryptor, CryptoStreamMode.Read)
                let decryptedBytes = zero |> Array.init bytes.Length
                let bytesRead = cryptoStream.Read(decryptedBytes, 0, bytes.Length)
                decryptedBytes |> Array.take bytesRead
            | RSA (keySize,padding) ->
                let rsa = RSACng.Create()
                rsa.KeySize <- keySize
                rsa.Encrypt(bytes, padding)

        let encrypt algorithm = decode Utf8 >> encryptBytes algorithm >> Convert.ToBase64String
        let decrypt algorithm = Convert.FromBase64String >> decryptBytes algorithm >> encode Utf8
                

module Text =
    let private wordPattern = """([\[\]\w\d'<>="/])+"""
    let private whitespacePattern = @"(\s)+"
    let private punctuationPattern = """([,\.'"!?()])+"""
    let private whitespaceAndPunctuationPattern = """([^\[\]\w\d'<>="/])+"""

    let tokenize tokenPattern text =
        let regex = Regex(tokenPattern)        
        let rec getMatches (matchResult: Match) =
            seq {
                if matchResult.Success then 
                    yield matchResult.Value
                    yield! getMatches <| matchResult.NextMatch()
            }
        getMatches <| regex.Match(text)
        
    let getWords = tokenize wordPattern
    let getWordList = getWords >> Seq.toList
    let getWhitespace = tokenize whitespacePattern
    let getWhitespaceList = getWhitespace >> Seq.toList
    let getPunctuation = tokenize punctuationPattern
    let getPunctuationList = getPunctuation >> Seq.toList
    let getWhitespaceAndPunctuation = tokenize whitespaceAndPunctuationPattern
    let getWhitespaceAndPunctuationList = getWhitespaceAndPunctuation >> Seq.toList
    
    let parse text =
        (getWords text, getWhitespaceAndPunctuation text)
    
    let parseLists text =
        (getWordList text, getWhitespaceAndPunctuationList text)

    let reconstruct words spaces =
        Seq.zip words spaces
        |> Seq.fold (fun acc (word,space) -> sprintf "%s%s%s" acc word space) System.String.Empty


module Stateful =
     type OneShotFlag(?initialState) =    
        let mutex = new SemaphoreSlim(1)
        let initialState = defaultArg initialState false
        let getState () = 
            let state = (seq { yield initialState 
                               while true do yield not initialState }).GetEnumerator()        
            (fun _ -> if state.MoveNext() then state.Current else not initialState)
        let state = getState()
        member flag.IsSet () = 
            mutex.Wait()
            try state()
            finally mutex.Release() |> ignore

    type IdSequence () =
        let lockObject = obj()
        let idSequence = seq {
            for id in {1..Int32.MaxValue} do
                yield id
        }
        let enumerator = idSequence.GetEnumerator()
        member __.NextId () =
            lock lockObject (fun _ ->
                if enumerator.MoveNext()
                then enumerator.Current
                else -1)
        member this.Next with get () = this.NextId()