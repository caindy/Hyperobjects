namespace Relay.Hyperobjects
open FSharp.Data

module Intents =
  type DataDefinition =
  | Element   of name:string * validation:Validation option
  | Composite of name:string * DataDefinition list
  and Validation =
  | Required
  | Typed of Primitive
  | Regex of string
  | Bounded of Bound
  | Validation of Validation list
  and Bound =
  | LessThan of int
  | LessThanOrEqual of int
  | GreaterThan of int
  | GreaterThanOrEqual of int
  | Range of lower:Bound * upper:Bound
  and Primitive = // taken from Swagger spec
  | Integer | Long | Float | Double | String | Byte | Binary
  | Boolean | Date | DateTime | TimeSpan | Password
  | Revision | UriString
  | Array of Primitive
  | UUIDv4

  // Cribbed from
  // https://gist.github.com/eulerfx/68975495f41bc3ce5683
  let bind f (r:ParseResult<'a>) : ParseResult<'b> =
    match r with
    | Success a -> f a
    | Failure err -> Failure err

  type JsonParserBuilder() =
      member x.Return a = Success a
      member x.ReturnFrom r = r
      member x.Bind (inp:ParseResult<'a>, body:'a -> ParseResult<'b>) : ParseResult<'b> = bind body inp

  let intent = new JsonParserBuilder()
  let inline (?) (j : JsonValue) (k : string) =
    match j with
    | JObject props -> jget props k
    | _ -> Failure <| sprintf "%s not found in %A" k j

  type Parser<'i> = JsonValue -> 'i ParseResult
  type Intent =  {
    Activity    : string
    TrackingId  : string
    Data        : JsonValue }
  type JsonValue with
    member x.Item
      with get(idx : string) =
        match x with
        | JsonValue.Record props -> props |> Seq.find (fst >> ((=) idx)) |> snd
        | _ -> failwithf "Couldn't find %s" idx

  open System
  let parseUrl s = intent {
    return!
      if Uri.IsWellFormedUriString(s, UriKind.Absolute) then Success <| System.Uri(s)
      else Failure <| sprintf "%s is not a URL" s
  }

  open System
  open System.IO
  open System.Net.Http
  open System.Net.Http.Headers
  open System.Net.Http.Formatting
  type IntentMediaTypeFormatter () as this =
    inherit BufferedMediaTypeFormatter()
    let hyperobjectsIntent = "hyperobjects/intent"
    do this.SupportedMediaTypes.Add(new MediaTypeHeaderValue(hyperobjectsIntent))

    override __.CanReadType(t: Type) : bool =
      typeof<Intent> = t || typeof<Intent seq>.IsAssignableFrom(t)
    override __.CanWriteType(t: Type) : bool = false
    override __.ReadFromStream(t : Type, s : Stream, _ : HttpContent, _ : IFormatterLogger) =
      let parse p =
        let j = intent {
          let! a = p?activity
          let! t = p?trackingId
          let! d = p?data
          let d' = defaultArg d JsonValue.Null
          return { Activity = a; TrackingId = t; Data = d' } }
        match j with
        | Success i -> i
        | Failure f -> failwithf "Bad Request, expected valid hyperobjects/intent, got %A" f
      use rdr = new StreamReader(s)
      match rdr.ReadToEnd() |> JsonValue.Parse with
      | JsonValue.Array a -> a |> Array.map parse :> _
      | a -> parse a :> _