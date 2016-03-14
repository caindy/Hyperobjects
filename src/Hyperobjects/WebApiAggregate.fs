namespace Relay.Hyperobjects

open System
open Aggregates
open Intents
open System.Net
open System.Net.Http
open System.Web.Http
open Relay

module WebApiAggregate =

  type RouteWithIdAndMethod = { id: Guid; httpMethod: string }

  [<CLIMutable>]
  type QueryRaw = {
    Activity   : string
    Instigator : string
    Since      : Nullable<DateTime>
    Before     : Nullable<DateTime>
    Limit      : Nullable<byte> }

  [<AbstractClass>]
  type Aggregate<'i,'e,'s> when 'e : equality
                           and  's : equality (a : Aggregate<uint16,Guid,'i,'e,'s>) =
    inherit ApiController()

    let normalizeReason (s : string) =
      s.Replace(Environment.NewLine, " ")
       .Replace('\n', ' ')

    member private x.MapFailure fail : IHttpActionResult =
      match fail with
      | Error _ -> upcast x.InternalServerError() // TODO bubble message
      | BadRequest m -> upcast x.BadRequest(normalizeReason m)
      | Busy -> upcast x.StatusCode(HttpStatusCode.ServiceUnavailable)
      | Forbidden m ->
        let response = new HttpResponseMessage(HttpStatusCode.Forbidden)
        response.ReasonPhrase <- normalizeReason m
        upcast x.ResponseMessage(response)
      | NotFound -> upcast x.NotFound()
      | Archived -> upcast x.StatusCode(HttpStatusCode.Gone)
      | Unauthorized m ->
        let response = new HttpResponseMessage(HttpStatusCode.Unauthorized)
        m |> Option.iter (fun m -> response.ReasonPhrase <- normalizeReason m)
        upcast x.ResponseMessage(response)

    [<NonAction>]
    member x.MapResponse result : IHttpActionResult =
      match result with
      | Ok (Resource r) -> upcast x.Ok(r)
      | Ok r -> upcast x.Ok(r)
      | Created r ->
        let LocalId guid, _ = r.Revision
        upcast x.CreatedAtRoute("AggregateRootRoute", { id = guid; httpMethod = "GET" }, r)
      | Fail other -> x.MapFailure other

    [<NonAction>]
    abstract GetInstigator : unit -> Instigator

    abstract Get : Guid -> IHttpActionResult
    default x.Get (id : Guid) =
      a.GetCurrent (x.GetInstigator()) id
      |> x.MapResponse

    member x.Get (id : Guid, revision : uint16) =
      a.GetRevision (x.GetInstigator()) id revision
      |> x.MapResponse

    member x.Post (i : Intent) =
      a.PostToAggregate (x.GetInstigator()) i
      |> x.MapResponse

    abstract Post : Guid * Intent -> IHttpActionResult
    default x.Post (id : Guid, i : Intent) =
      a.PostToCurrent (x.GetInstigator()) id i
      |> x.MapResponse

    member x.Post (id : Guid, revision : uint16, i : Intent) =
      a.Fork (x.GetInstigator()) id revision i
      |> x.MapResponse

    [<AcceptVerbs("POSIT")>]
    member x.Posit (id: Guid, intent: Intent) =
      a.PositCurrent (x.GetInstigator()) id intent
      |> x.MapResponse

    [<AcceptVerbs("POSIT")>]
    member x.Posit (id : Guid, revision : uint16, intent : Intent) =
      a.PositRevision (x.GetInstigator()) id revision intent
      |> x.MapResponse

    [<AcceptVerbs("POSIT")>]
    member x.BatchPosit (id : Guid, revision : uint16, [<FromBody>] intents : Intent array) =
      a.BatchPositRevision (x.GetInstigator()) id revision intents
      |> x.MapResponse

    [<NonAction>] // [<AcceptVerbs("POSIT")>]
    member x.Posit (id : Guid, revision : uint16, intentBatches : Intent array array) =
      let results = intentBatches |> Array.map (a.BatchPositRevision (x.GetInstigator()) id revision)
      let oks, notOks = results |> Array.partition (function Ok _ -> true | _ -> false)
      if notOks.Length = 0 then
        x.Ok(oks) :> IHttpActionResult
      else
        let fails, notFails = notOks |> Array.partition (function Fail _ -> true | _ -> false)
        System.Diagnostics.Debug.Assert(notFails.Length = 0) // we should never see e.g. Created
        x.MapFailure(Error <| Some (sprintf "There were %d other errors" fails.Length))

    member private __.ExecuteQuery(q : QueryRaw) =
      let limit = q.Limit |> Option.ofNullable |> orDefault 50uy
      match q.Activity, q.Instigator with
      | Activity u, i when isEmpty i ->
        a.Find u None limit |> Some
      | Activity u, Instigator i ->
        a.Find u (Some i) limit |> Some
      | _ -> None

    [<AcceptVerbs("QUERY")>]
    member x.Query(q : QueryRaw) : IHttpActionResult =
      match x.ExecuteQuery(q) with
      | Some r -> upcast x.Ok r
      | _ -> x.BadRequest(sprintf "'%s' not a valid Activity URN" q.Activity) :> _

    [<AcceptVerbs("QUERY")>]
    member x.Query(q : QueryRaw array) : IHttpActionResult =
      match q |> Array.map x.ExecuteQuery |> Array.choose id with
      | [||] -> x.BadRequest("No query contained a valid Activity URN") :> _
      | s ->
        match s |> Seq.concat |> Seq.distinct with
        | empty when Seq.isEmpty empty -> upcast x.Ok()
        | r -> upcast x.Ok r
