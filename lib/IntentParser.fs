// #r Hyperobjects.dll
module IntentParser

open System
open System.Collections.Generic
open FSharp.Data
open Relay
open Relay.Hyperobjects
open Relay.Hyperobjects.Fleece
open Relay.Hyperobjects.Intents
open FSharp.Quotations
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.ExprShape

let toValidation t =
  let rec toPrimitive t isOpt isList =
    let wrap primitive =
      let prim = if isList then Array primitive else primitive
      prim, isOpt
    match t with
    | id when id = typeof<Continuant * uint16> -> wrap Revision
    | uri when uri = typeof<Uri> -> wrap UriString
    | guid when guid = typeof<Guid> -> wrap UUIDv4
    | i when i = typeof<int> -> wrap Integer
    | b when b = typeof<bool> -> wrap Boolean
    | d when d = typeof<double> -> wrap Double
    | b when b = typeof<byte> -> wrap Byte
    | d when d = typeof<DateTime> -> wrap DateTime
    | ts when ts = typeof<TimeSpan> -> wrap TimeSpan
    | s when s = typeof<String> -> wrap String
    | us when us = typeof<uint16> -> wrap Integer // HACK
    | a when a.IsArray ->
      let arrayType = a.GetElementType()
      toPrimitive arrayType isOpt true
    | u when FSharpType.IsUnion u && u.IsGenericType && u.GetGenericTypeDefinition() = typedefof<option<_>> ->
      let optionType = u.GetGenericArguments() |> Seq.head
      toPrimitive optionType true isList
    | u when FSharpType.IsUnion u ->
      let cases = FSharpType.GetUnionCases u
      let firstProp = cases |> Seq.collect (fun c -> c.GetFields()) |> Seq.tryHead
      match firstProp with
      | None -> wrap String // HACK
      | Some p ->
        let isList = u.IsGenericType && u.GetGenericTypeDefinition() = typedefof<list<_>>
        toPrimitive p.PropertyType isOpt isList
    | tuple when FSharpType.IsTuple tuple -> wrap String // HACK shouldn't receive tuples (IngredientProduct = DocumentId * Guid)
    | _ -> failwithf "Unsupported Type: %A" t
  let prim, isOpt = toPrimitive t false false
  if not isOpt then Required :: [Typed prim] else [Typed prim] 

type DataDefinitionType =
  | Splice of methodName:string * Type
  | Concrete of name:string * Type
and DataDefinitionInterp = {
  DataDefinitionType : DataDefinitionType
  FromBind : string
  ToBind : string }
and AnalyzedIntentParser = {
  Name : string
  Interpretations : DataDefinitionInterp list }

type IntentQuote() =

  static let (|BuilderBind|_|) expr =
    match expr with
    | Call (Some _, mthd, exprs)
      when mthd.Name = "Bind" && mthd.DeclaringType = typeof<JsonParserBuilder> ->
      Some (exprs)
    | _ -> None

  static let (|ExternalCall|_|) exprs =
    match exprs with
    | [Call (None, m, [Var v]); e2] -> Some (m.Name, e2, v.Name)
    | [Application (ValueWithName (_,_,s), Var v); e2] -> Some (s, e2, v.Name)
    | [Application (PropertyGet (None, prop, []), Var v); e2] -> Some (prop.Name, e2, v.Name)
    | _ -> None

  static let (|LetBind|_|) expr =
    match expr with
    | Lambda(_, Let (letBind, _, exprs)) -> Some (letBind, exprs)
    | _ -> None

  static let (|ExternalBind|_|) expr =
    match expr with
    | BuilderBind (ExternalCall (mthdName, LetBind(letBind, exprs), mthdArg)) ->
      Some (letBind, mthdName, mthdArg, exprs)
    | _ -> None

  static let (|JsonBind|_|) expr =
    match expr with
    | BuilderBind ([Call (None, mthd, [Var v; Value (propName,_)]); LetBind(letBind, exprs)])
      when mthd.Name = "op_Dynamic" ->
      let name = string propName
      Some (name, v.Name, mthd, letBind, exprs)
    | _ -> None

  static member AnalyzeExpression(name, (e : Expr<JsonValue -> ParseResult<'a>>)) =
    let namespaced n = name + ":" + n
    let rec loop e =
      seq {
        match e with
        | ExternalBind (letBind, mthdName, mthdArg, expr) ->
          // value is bound to an external function call
          let splice = { DataDefinitionType = Splice (mthdName, letBind.Type)
                         FromBind           = namespaced mthdArg
                         ToBind             = namespaced letBind.Name }
          yield splice
          yield! loop expr
        | JsonBind (propName, varName, mthd, letBind, expr) ->
          // value is bound from a jget (?) call
          let returnType = mthd.ReturnType.GenericTypeArguments |> Seq.head
          let concrete = { DataDefinitionType = Concrete (propName, returnType)
                           FromBind           = namespaced varName
                           ToBind             = namespaced letBind.Name }
          yield concrete
          yield! loop expr
        | ShapeLambda (_,e) -> yield! loop e
        | ShapeVar _ -> ()
        | ShapeCombination (_, exprs) ->
          yield! exprs |> Seq.collect loop
      }
    let parseFunc = 
      match e with
      | WithValue (value, _, _) -> value :?> (JsonValue -> ParseResult<'a>)
      | _ -> failwithf "Expression return value is not of type ParseResult<_>: %A" e
    parseFunc, { Name = name; Interpretations = loop e |> List.ofSeq }

  static member Analyze(activityName, ([<ReflectedDefinition(true)>] e : Expr<JsonValue -> ParseResult<'a>>)) =
    IntentQuote.AnalyzeExpression(activityName, e)

type NameValueCollection<'a> =
| NameValue of 'a * NameValueCollection<'a> list
| Value of 'a

let rec treeify (props : DataDefinitionInterp list) =
  match props with
  | x::xs ->
    let children, rest = xs |> List.partition (fun d -> d.FromBind = x.ToBind)
    if children.IsEmpty then
      Value x.DataDefinitionType :: treeify rest
    else
      let childLeaves = treeify children
      NameValue (x.DataDefinitionType, childLeaves) :: treeify rest
  | _ -> []

let rec toDataDefinition (others: AnalyzedIntentParser list) tree =
  match tree with
  | NameValue (Concrete (n,_), vt) ->
    let children = vt |> List.collect (toDataDefinition others)
    [Composite(n, children)]
  | Value d ->
    let elem n t = [Element (n, Some << Validation <| toValidation t)]
    match d with
    | Concrete (n,t) -> elem n t
    | Splice (n,t) ->
      let other = others |> List.tryFind (fun o -> o.Name = n)
      match other with
      | None -> [] // HACK failwithf "Couldn't find AnalyzedParserIntent for '%s'" n
      | Some other ->
          let otherTree = treeify other.Interpretations
          match otherTree with
          | [x] ->
              let dataDef = toDataDefinition others x
              match dataDef with
              | [Element (n,_)] -> elem n t
              | _ -> dataDef
          | xs -> xs |> List.collect (toDataDefinition others)
  | _ -> failwith "I didn't ask for this" // HACK refactor so this case is impossible?

let toDataDefinitions analyzed others =
  analyzed.Interpretations
  |> List.distinct // HACK some properties are ref'd multiple times, in branches
  |> treeify
  |> List.collect (toDataDefinition others)

type IntentParserRegistry<'a>(activityfier, analyzedParserDependencies) =
  let parsers = Dictionary<Activity, JsonValue -> ParseResult<'a>>()
  let analyzedParsers = List<Activity * DataDefinition list>()

  member __.AnalyzedParsers with get() = analyzedParsers

  member __.Register(activityName, ([<ReflectedDefinition(true)>] e : Expr<JsonValue -> ParseResult<'a>>)) =
    let activity = activityName |> activityfier
    let parse, analyzed = IntentQuote.AnalyzeExpression(activityName, e)
    parsers.Add(activity, parse)
    analyzedParsers.Add(activity, toDataDefinitions analyzed analyzedParserDependencies)

  member __.GetParser(activity) = parsers.TryGet activity

let json (defs : DataDefinition list) =
  let getRecord value =
    match value with
    | JsonValue.Record r -> Some r
    | _ -> None

  let rec strSample validation =
    let getType prim =
      let str s = JsonValue.String s
      match prim with
      | Array p ->
        let values = seq {1..3} |> Seq.map (fun _ -> strSample <| Typed p)
        JsonValue.Array <| Array.ofSeq values
      | Boolean -> JsonValue.Boolean true
      | String -> str "string"
      | DateTime -> str <| DateTime.UtcNow.ToString "O"
      | TimeSpan -> str <| (TimeSpan.FromHours 1.234).ToString() // TODO use real format
      | Byte -> JsonValue.Number 255m
      | Integer | Long -> JsonValue.Number 123456m
      | Float | Double -> JsonValue.Number 1.23456m
      | UriString -> str "http://www.relayfoods.com"
      | Revision -> str "<revision>"
      | UUIDv4 -> str <| string (Guid.NewGuid())
      | _ -> str "unknown"

    match validation with
    | Validation list ->
        match list with
        | [] -> JsonValue.Null
        | vs ->
          let required = vs |> Seq.exists (function Required -> true | _ -> false)
          let typed = vs |> Seq.choose (function Typed p -> Some p | _ -> None) |> Seq.tryHead
          match typed with
          | None -> JsonValue.Null
          | Some p ->
            let value = getType p
            if required then value else JsonValue.String (value.AsString() + " (optional)")
    | Typed p -> getType p
    | _ -> failwith "I didn't ask for this"
  
  let getRecord = Seq.choose getRecord >> Seq.collect id >> Array.ofSeq >> JsonValue.Record

  let rec loop (def : DataDefinition) =
    match def with
    | Element (n, Some v) -> JsonValue.Record [|(n, strSample v)|]
    | Element (n, None) -> JsonValue.Record [|(n, JsonValue.Null)|]
    | Composite (n,ds) ->
      let childJson = ds |> List.map loop
      let record = getRecord childJson
      JsonValue.Record [|(n, record)|]

  let jsons = defs |> List.map loop
  getRecord jsons
