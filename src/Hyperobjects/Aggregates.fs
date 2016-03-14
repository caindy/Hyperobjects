namespace Relay.Hyperobjects
 
open System
open System.Collections.Generic
open Relay

module Aggregates =
  [<CLIMutable>]
  type ResourceResponse<'s> = {
    Links      : IDictionary<string, Uri>
    Forks      : IDictionary<string, Uri>
    Body       : 's
    Revision   : Revision
    CreatedBy  : Instigator
    Instigator : Instigator
    TrackingId : string }

  let private getCreator r =
    let firstEvent = r.Events |> Seq.head
    match firstEvent.Operation with
    | Created _ | ForkOf _ | Originated _->
      let creator = firstEvent.Instigator
      Success creator
    | _ -> Failure "First event must be creation or fork"

  let private anon = Uri "urn:hyperobjects:principals:anonymous"
  let Anonymous = User anon.AbsoluteUri

  let Service name = urn ("urn:hyperobjects:principals:service:" + name)

  let (|InstigatorUri|) = function User u -> Uri u
  let (|Instigator|_|) = function
  | IsUrl u
  | IsUrn u when u.AbsoluteUri <> anon.AbsoluteUri -> User u.AbsoluteUri |> Some
  | _ -> None

  let response r =
    let lastEvent = r.Events |> Seq.last
    let creator =
      match getCreator r with
      | Success c -> c
      | Failure msg -> failwith msg
    let revisionId = r.RevisionId
    let (ContinuantUrl continuantId) = r.Id
    // include forked continuants
    let forks =
      r.Events
      |> Seq.choose (fun e -> match e.Operation with
                              | ForkedBy (ContinuantUrl c, title) -> Some (title,c)
                              | _ -> None)
    let baseResponse = {
      Body = r.State
      Links = rwdict [ ("revision", revisionId); ("continuant", continuantId) ]
      Forks = forks |> dict
      Revision = r.Revision
      CreatedBy = creator
      Instigator = lastEvent.Instigator
      TrackingId = "" }

    let historyUrl = sprintf "%s/history" revisionId.AbsoluteUri
    baseResponse.Links.Add("history", Uri(historyUrl))
    baseResponse.Links.Add("createdBy", let (InstigatorUri u) = creator in u)

    // if forked, include the antecedent's url
    let firstEvent = r.Events |> Seq.head
    let addAntecedent u =
      baseResponse.Links.Add("antecedent", u)
    let addOriginator u =
      baseResponse.Links.Add("originator", u)
    match firstEvent.Operation with
    | ForkOf (RevisionUrl u, _) -> addAntecedent u
    | Originated { Originator = (RevisionUrl o);  Antecedent = Some (RevisionUrl a); Initial = _ } ->
      addAntecedent a; addOriginator o
    | Originated { Originator = (RevisionUrl o);  Antecedent = _; Initial = _ } ->
      addOriginator o
    | _ -> ()

    match lastEvent.Impetus with
    | DirectCause (External s) ->
      { baseResponse with TrackingId = s }
    | _ -> baseResponse

  type HistoryResponse<'s,'e> = {
    Links : IDictionary<string,Uri>
    Body : Event<'s,'e> seq }

  type Query =
  | ActivitySince of Activity * Timestamp
  | ActivityBefore of Activity * Timestamp
  | ActivityBetween of Activity * Timestamp * Timestamp
  | Instigated of Uri * Activity

  type Response<'s,'e> =
  | Resource of ResourceResponse<'s>
  | History of HistoryResponse<'s,'e>
  | QueryResult of IDictionary<string,string> array

  type Failures =
  | Error of string option
  | Forbidden of string
  | Unauthorized of string option
  | Archived
  | Busy
  | NotFound
  | BadRequest of string

  type OperationResult<'a> = Choice<'a,Failures>
  let bind (m : 'a -> OperationResult<'b>) (result : OperationResult<'a>) =
    match result with
    | Success a -> m a
    | Failure f -> Failure f
  let inline (>>=) a f = bind f a

  type AggResult<'id,'s,'e> =
  | Ok of Response<'s,'e>
  | Created of ResourceResponse<'s>
  | Fail of Failures

  type Origination<'s> = private {
    Initial    : 's
    Originator : Revision
    Antecedent : Revision option
    Instigator : Instigator
    Impetus    : Causality }

  type ConjugateContext<'s,'e> = {
    Instigator : Instigator
    Creator    : Instigator
    Impetus    : Causality
    Revision   : Revision
    History    : Event<'s,'e> array } with
    member ctxt.Originate s antecedent : Origination<'childState> =
      { Initial = s; Originator = ctxt.Revision; Antecedent = antecedent;
        Instigator = ctxt.Instigator; Impetus  = ctxt.Impetus }

  [<Flags>]
  type Access = None = 0 | Read = 1 | Write = 2 | Full = 3

  let private canRead  (access : Access) : bool = Access.Read  &&& access = Access.Read
  let private canWrite (access : Access) : bool = Access.Write &&& access = Access.Write

  type IAggregate<'i,'e,'s> when  'e : equality
                            and   's : equality =
    abstract member Activity      : string -> Activity
    abstract member EntityId      : Guid -> Continuant
    abstract member Empty         : 's
    abstract member IntentParser  : Activity -> Intents.Parser<'i> option
    abstract member Conjugate     : 's -> 'i -> ConjugateContext<'s,'e> option -> OperationResult<'e>
    abstract member Authorize     : creator:Instigator * reader:Instigator * state:'s -> Access
    abstract member Apply         : 's -> 'e -> 's
    abstract member NewFork       : 's -> 'i -> (string * 'e) option

  open Intents
  type Limit = byte
  type ActivityHistory = (Revision * Timestamp) seq
  type Modification<'id,'rev,'e> = {
    Instigator  : Instigator
    Activity    : Activity
    LocalId     :'id
    Revision    : 'rev
    DomainEvent : 'e
    Impetus     : Causality }

  type Aggregate<'rev,'id, 'i, 'e,'s> when 'e : equality
                                      and  's : equality = {
    GetCurrent          : Instigator -> 'id           -> AggResult<'id,'s,'e>
    GetRevision         : Instigator -> 'id -> 'rev   -> AggResult<'id,'s,'e>
    PostToAggregate     : Instigator ->        Intent -> AggResult<'id,'s,'e>
    PostToCurrent       : Instigator -> 'id -> Intent -> AggResult<'id,'s,'e>
    PositCurrent        : Instigator -> 'id -> Intent -> AggResult<'id,'s,'e>
    Fork                : Instigator -> 'id -> 'rev   -> Intent       -> AggResult<'id,'s,'e>
    PositRevision       : Instigator -> 'id -> 'rev   -> Intent       -> AggResult<'id,'s,'e>
    BatchPositRevision  : Instigator -> 'id -> 'rev   -> Intent array -> AggResult<'id,'s,'e>
    Find                : Activity   -> Instigator option -> Limit -> ActivityHistory
    Modify              : Modification<'id,'rev,'e>   -> OperationResult<Entity<'s,'e>>}

  type ISubordinate<'i,'e,'s> =
    abstract member Parser    : Activity -> Intents.Parser<'i> option
    abstract member Conjugate : 's -> 'i -> OperationResult<'e>
    abstract member Apply     : 's -> 'e -> 's
    abstract member Get       : Revision -> Entity<'s,'e> option
    abstract member Get       : Uri -> Entity<'s,'e> option
    abstract member Originate : Origination<'s> -> Entity<'s,'e> option

  let subordinateFactory (this : IAggregate<'i,'e,'s>) (eventStore : EventStore<'s,'e>)  : ISubordinate<'i,'e,'s> =
    { new ISubordinate<'i,'e,'s> with
      member __.Parser a = this.IntentParser a
      member __.Conjugate s i = this.Conjugate s i None
      member __.Apply s e  =  this.Apply s e
      member __.Get (rev : Revision) = eventStore.Get(EntityId.Version rev)
      member __.Get (uri : Uri) =
        match uri with
        | InvalidEntityId -> None
        | VersionId c
        | CurrentId c -> eventStore.Get(c)
      member __.Originate { Initial = initial; Instigator = i; Impetus = cause; Originator = originator; Antecedent = maybeantecedent } =
        let newId = Guid.NewGuid()
        let activity = this.Activity "originate"
        let op = Operation.Originated { Originator = originator; Antecedent = maybeantecedent; Initial = initial }
//        let (Name.Urn activityUrn) = activity
        let initial = {
          EntityId  = this.EntityId newId
          Activity = activity
          Operation = op
          Timestamp = utcTimestamp()
          Instigator = i
          Revision = 0us
          Impetus = cause }
        eventStore.Store initial
        eventStore.Get <| Current initial.EntityId }

  let aggregateFactory (this : IAggregate<'i,'e,'s>) (eventStore : EventStore<'s,'e>) : Aggregate<uint16,Guid,'i,'e,'s> =

    let getEntity (id : Guid) (revision : uint16 option) =
      let entityId = this.EntityId id
      match revision with
      | Some r ->
        eventStore.Get <| EntityId.Version (entityId, r)
      | None ->
        eventStore.Get <| Current entityId

    let authorizedGet instigator id rev : OperationResult<Entity<'s,'e>> =
      match getEntity id rev with
      | Some r ->
        let lastEvent = r.Events |> Seq.last
        if lastEvent.Operation = Operation.Archived then
          Failure <| Failures.Archived
        elif this.Authorize(r.Creator, instigator, r.State) |> canRead then
          Success r
        else
          Failure <| Failures.Forbidden (sprintf "%A not allowed access to resource %A" instigator id)
      | None -> Failure <| Failures.NotFound
    let ok = function Success (r : Entity<'s,'e>) -> Ok (Resource (response r))
                    | Failure f -> Fail f
    let badRequest = Failure << Failures.BadRequest
    let getById instigator id = authorizedGet instigator id None |> ok
    let getByRevision instigator id revision = authorizedGet instigator id (Some revision) |> ok

    let externalCause (intent : Intent) = DirectCause <| External intent.TrackingId
    let newAggregateRoot id instigator cause activity (op : Operation<'s,'e>) : OperationResult<Entity<'s,'e>> =
      let initial = {
        EntityId  = this.EntityId id
        Activity = activity
        Operation = op
        Timestamp = utcTimestamp()
        Instigator = instigator
        Revision = 0us
        Impetus = cause }
      eventStore.Store initial
      match eventStore.Get <| Current initial.EntityId with
      | Some r -> Success r
      | None -> Failure <| Error None

    let makeNewEvent (r : Entity<'s,'e>) activity (e : 'e) instigator cause =
      let last = r.Events |> Seq.last
      { EntityId = r.Id
        Activity = activity
        Operation = Domain e
        Impetus = cause
        Timestamp = utcTimestamp()
        Instigator = instigator
        Revision = last.Revision + 1us }

    let processEvent entityId evt =
      eventStore.Store evt
      match eventStore.Get <| Current entityId with
      | Some n -> Success n
      | None -> Failure <| Failures.Error None

    let archive (r : Entity<'s,'e>) instigator cause =
      { EntityId  = r.Id
        Activity = this.Activity "archive"
        Operation = Operation.Archived
        Timestamp = utcTimestamp()
        Instigator = instigator
        Revision = (snd r.Revision) + 1us
        Impetus = cause }

    let mkCtxt u cause (r : Entity<'s,'e>) =
      Some { Instigator = u; Creator = r.Creator; Impetus = cause;
             Revision = r.Revision; History = Array.ofSeq r.Events }

    /// process intents from external actors
    let rootOp r instigator (i : ParsedIntent<'i>) =
      let cause = DirectCause <| External i.Intent.TrackingId
      if r.Lock.Wait(millisecondsTimeout = 60) then
        try
          this.Conjugate r.State i.Parsed (mkCtxt instigator cause r)
          >>= fun e -> makeNewEvent r (urn i.Intent.Activity) e instigator cause
                       |> processEvent r.Id
        finally
          r.Lock.Release() |> ignore
      else
        Failure <| Failures.Busy

    /// process events generated by the system, not Intents from external actors
    let internalRootOp r activity domainEvent instigator cause =
      if r.Lock.Wait(millisecondsTimeout = 60) then
        try
          makeNewEvent r activity domainEvent instigator cause
          |> processEvent r.Id
        finally
          r.Lock.Release() |> ignore
      else
        Failure <| Failures.Busy

    let interpret (intent: Intent) (apply: ParsedIntent<'i> -> OperationResult<Entity<'s,'e>>) : OperationResult<Entity<'s,'e>> =
      if isNull (box intent) then
        badRequest "An intent is required"
      else
        match this.IntentParser(urn intent.Activity) with
        | Some p ->
          match p(intent.Data) with
          | Success i -> apply { Intent = intent; Parsed = i }
          | Failure s -> badRequest s
        | _ -> badRequest <| sprintf "%s is not a valid Intent" intent.Activity

    let processRootOp (intent : Intent) instigator (r : Entity<'s,'e>) =
      if not << canWrite <| this.Authorize(r.Creator, instigator, r.State) then
        Failure <| Failures.Forbidden (sprintf "%A not allowed write access to resource %A" instigator id)
      else
        if intent.Activity = string (this.Activity "archive") then
          let cause = DirectCause <| External intent.TrackingId
          archive r instigator cause |> processEvent r.Id
        else
          rootOp r instigator |> interpret intent

    let postToAggregateRoot instigator id intent : AggResult<Guid,'s,'e> =
      authorizedGet instigator id None
      >>= processRootOp intent instigator
      |> ok

    let forkOp (intent : Intent) instigator r =
      if isNull (box intent) then badRequest "An intent is required"
      else
      let newId = Guid.NewGuid()
      match this.IntentParser(urn intent.Activity) with
      | Some p ->
        match p(intent.Data) with
        | Failure s -> badRequest s
        | Success i ->
          let title, forkEvent =
            match this.NewFork r.State i with
            | None ->
              let uniqueChildTitle = string <| Guid.NewGuid()
              uniqueChildTitle, None
            | Some (title, evt) -> title, Some evt
          let forkedBy =
            let last = r.Events |> Seq.last
            { EntityId = r.Id
              Activity = this.Activity "forked"
              Operation = ForkedBy (this.EntityId newId, title)
              Impetus =  DirectCause <| External intent.TrackingId
              Timestamp = utcTimestamp()
              Instigator = instigator
              Revision = last.Revision + 1us }
          eventStore.Store forkedBy
          let newRevision = fst r.Revision, forkedBy.Revision
          newAggregateRoot newId instigator (externalCause intent) (this.Activity "fork") (ForkOf (newRevision, forkEvent))
      | _ -> badRequest <| sprintf "%s is not a valid Intent" intent.Activity

    let fork instigator id revision (intent : Intent) =
      authorizedGet instigator id (Some revision)
      >>= forkOp intent instigator
      >>= fun r -> rootOp r instigator |> interpret intent
      |> function Success r -> Created (response r) | Failure f -> Fail f

    let posit (r : Entity<'s,'e>) instigator (i : ParsedIntent<'i>) : OperationResult<Entity<'s,'e>> =
      let cause = DirectCause <| External i.Intent.TrackingId
      this.Conjugate r.State i.Parsed
                     <| mkCtxt instigator cause r
      >>= fun e ->
          let evt = makeNewEvent r (urn i.Intent.Activity) e instigator cause
          Success <| eventStore.Apply r evt

    let positAggregateRoot instigator id intent =
      authorizedGet instigator id None
      >>= fun r -> interpret intent (posit r instigator)
      |> ok

    let positRevision instigator id rev intent =
      authorizedGet instigator id (Some rev)
      >>= fun r -> interpret intent (posit r instigator)
      |> ok

    let batchPositRevision instigator id rev intents =
      let entity = authorizedGet instigator id (Some rev)
      intents |> Seq.fold (fun e i -> e >>= (fun r -> interpret i (posit r instigator))) entity
      |> ok

    let postToLogicalAggregate instigator (intent : Intent) =
      if isNull (box intent) then // boxing is necessary because we don't control the HTTP layer
        Failures.BadRequest "An intent is required" |> Fail
      else
        let newId = Guid.NewGuid()
        let supportedCreate = string (this.Activity "create")
        let r =
          if intent.Activity = supportedCreate then
            newAggregateRoot newId instigator (externalCause intent) (urn intent.Activity) (Operation.Created this.Empty)
          else
            sprintf "'%s' is unsupported; only %s is supported at this time. Received %A" intent.Activity supportedCreate intent
            |> badRequest
        r |> function Success r -> Created (response r) | Failure f -> Fail f

    let find activity instigator limit =
      match instigator with
      | Some instigator -> eventStore.FindByInstigatorActivity instigator activity (Some limit)
      | None            -> eventStore.FindByActivity activity (Some limit)
      |> Seq.sortByDescending snd

    let modify { Instigator = instigator; Activity = activity;
                 LocalId = id; Revision = rev; DomainEvent = domainEvent; Impetus = cause } =
      let continuant = this.EntityId id
      let (revision : Revision) = (continuant, rev)
      authorizedGet instigator id None
      >>= fun r ->
          // ensure current is same as revision, as modification decisions
          // should be made only based on the latest revision
          if r.Revision <> revision then // i.e. ChangeConflictException on an implicit action
            Some >> Failures.Error >> Failure <|
            sprintf "An attempt to modify %A at revision %d failed because the latest revision is now %d" continuant rev (snd r.Revision)
          else
            internalRootOp r activity domainEvent instigator cause

    { GetCurrent = getById
      PostToAggregate = postToLogicalAggregate
      PostToCurrent = postToAggregateRoot
      PositCurrent = positAggregateRoot
      GetRevision = getByRevision
      PositRevision = positRevision
      BatchPositRevision = batchPositRevision
      Fork = fork
      Find = find
      Modify = modify }
