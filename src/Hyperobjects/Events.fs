namespace Relay.Hyperobjects

open System
open Relay

(*
  Hyperobject, n. microservice defined by a Bounded Context, with event-sourced Entities
  and Aggregates exposed as a REST resources, adhering to the REST hypermedia constraint,
  and incorporating elements of CEP including timestamps and causality. All inbound messages
  to the resources are codified as Intents, extending the notion of the REST uniform interface
  constraint to include the message format.

  A key goal is enabling the [operationalization of business domains](https://en.wikipedia.org/wiki/Operationalization).
  Rather than defining the entities of the domain in a priori in a rigid way (e.g. defining a database schema),
  the goal is to allow the understanding of the domain to be steadily refined by defining operations (Intents)
  that the business intends to carry out. In this way the process of evolving a service proceeds naturally
  with the knowledge crunching of DDD.

  The Intent of an Instigator is first _parsed_. This parsed intent is then interpreted. If
  the parsed intent is determined to be a domain operation, it is conjugated by the domain implementation
  (see `Operation.Domain`). Conjugation is essentially the function `State -> ParsedIntent -> Event`, and
  is intended to convey the notion of translating a present intent to a past event.
  Once an event is determined, it is applied via a domain implementation and subsequently stored. The state
  of the resource as defined by the application of the event is then projected by the specific implementation.

  A useful simplification is that a Hyperobject is a service that provides the following pipeline for
  URL identified DDD aggregates:
  `Parse (intent) --> Conjugate (intent) --> Apply (event) --> Store (event) --> Project (state)`
*)

[<AutoOpen>]
module Events =

  /// An Event is the only object that is durable. All state is projected from the service by
  /// folding Apply over a sequence of events.
  type Event<'s,'e> = {
    EntityId      : Continuant
    Revision      : uint16
    Activity      : Activity
    Operation     : Operation<'s,'e>
    Instigator    : Instigator
    Impetus       : Causality
    Timestamp     : MachineClock }

  /// Continuant is simply a URL. Its name is intended to convey the notion that it identifies
  /// a unique resource as it changes over time. (The term is borrowed from Aristotle's Physics.)
  and Continuant = Url of string

  /// Revision identifies a point in the timeline of a resource. As such it is a tuple of the Continuant
  /// identifying that resource paired with a strictly increasing natural number providing a total ordering
  /// to all the events in the timeline.
  and Revision = Continuant * uint16

  /// Operation represents all the possible changes for a given resource. Of these, only the semantics of Domain
  /// are defined by the domain implementation, the rest are defined by Hyperobjects.
  /// The type parameter 's represents a reified state record in the domain; 'e is a domain defined event (usually
  /// a discriminated union).
  and Operation<'s,'e> =
  /// Represents the beginning of a new timeline with the initial value (empty) of 's
  | Created    of 's
  /// The operationalization of the Aggregate
  | Domain     of 'e
  /// Always stored concommitantly with `ForkedBy`, this represents the beginning of a new timeline (like `Created`)
  | ForkOf     of Revision * 'e option
  /// Always stored concommitantly with `ForkOf`, this represents the point in a continuant's timeline where
  /// another Aggregate forked off. The semantics of forking are a domain concern.
  | ForkedBy   of Continuant * string
  /// This Operation allows an Aggregate to create a new Aggregate from a Subordinate Entity, i.e. a new Aggregate
  /// is originated at a point in the timeline of its former parent. The semantics of origination are a domain concern.
  | Originated of Origination<'s>
  /// The durability of Hyperobjects is centered on the concept of an immutable event store. As such, there is no
  /// delete operation. The semantics of archival imply the entity is no longer accessible, not that it does not exist.
  | Archived
  (* Additional operations to consider:
     | Reset of 's // would allow for PUT operations against Aggregates, i.e. resetting the state of an entity
     | Unarchive   // would put the entity back in play, so to speak
   *)

  /// Used in `Operation.Originated`, this allows the new entity to know where it came from and to take on
  /// an initial value determined by it's birthplace
  and Origination<'s> = { Originator : Revision; Antecedent : Revision option; Initial : 's }

  /// Why did this event take place? It's one of the hardest parts of debugging, and is exacerbated
  /// terribly in distributed systems. The Impetus of an Event (defined as kind of Causality) is intended to make this explicit.
  and Causality =
  /// The simplest Causality is one with a single DirectCause
  | DirectCause     of EventIdentifier
  /// If multiple events taken together represent an Impetus for some Event, we call it a CausalSet
  | CausalSet       of EventIdentifier array
  (*
    Remarks on Causality: This DU was intended to match the ontology described in The Power of Events by David Luckham.
    Here we have generalized to only two kinds of impetus. This ontology should be extended once events begin arriving
    from other systems, e.g. consider an Impetus triggered by a partially ordered set of external events (A -> A') & B,
    i.e. when event A is seen, followed by an event A' along with a B, then create a new Event locally. This seems a little
    abstract, so A = check out cart, A' = update cart, B = cancel order.

    The main idea here is that a microservice (i.e. "bounded context with a memory") can respond to arbitrary sets of external
    events over time. Contrast with a a nanoservice (i.e. "reactive abstraction with no memory").
  *)

  /// Timestamps needn't be simple simple values. This is another concept from Luckham, and again this is about
  /// making Causality explicit, and explicitly traceable.
  and Timestamp =
  | Simple   of Clock
  | Interval of Clock * Clock // consider the (A -> A') & B example above, this would be e.g. (time(A), max(time(A') time(B))
  | Complex  of Timestamp array

  /// Clocks don't make sense without an idea of where they came from... the idea here is that
  /// for any given machine, its clock should be monotonically increasing, but the true source of
  /// time is the Revision in a given timeline. All the same, wall clocks are very useful to get
  /// a general sense of when things happened.
  and Clock =
  | Machine of MachineClock
  | Logical of SequenceIdentifier*Name
  and MachineClock = Utc of time:DateTime*machine:string

  /// This should probably be ImpetusIdentifier
  and EventIdentifier =
  | External of OpaqueIdentifier
  | Internal of SequenceIdentifier
  and SequenceIdentifier = uint64
  and OpaqueIdentifier = string

  /// An Activity nominates both Intents and Events. It's a URN used both to select the semantics
  /// of the intents sent to the Hyperobject (informing the parse) as well as labeling the Operation
  /// of the consequent Event. The latter is useful if the events are propagated to a bus where
  /// the Activity URN can serve as a Topic for subscriptions.
  and Activity = Name
  and Name = internal Urn of string with
    override x.ToString() = let (Urn u) = x in u

  /// an actor in the system nominated as the instigator of an event
  and Instigator = User of string

  /// Projects the Timestamp into a comparable string
  let (|TimestampStr|) = function
  | Simple (Machine (Utc (t,_))) -> t.ToString("o")
  | t -> failwithf "Timestamp format not implemented %A" t

  let (|IsUrl|IsUrn|NotUri|) = function
  | AbsoluteUri u when u.Scheme = "urn" -> IsUrn u
  | AbsoluteUri u -> IsUrl u
  | _ -> NotUri

  let (|LocalId|) (c : Continuant) = let (Url u) = c in Uri(u).LastPart |> Guid.Parse

  let continuant = function IsUrl u -> Url u.AbsoluteUri | s -> failwithf "%s is not a valid URL" s
  let urn =        function IsUrn u -> Urn u.AbsoluteUri | s -> failwithf "%s is not a valid URN" s
  let (|Activity|_|) = function IsUrn u -> Urn u.AbsoluteUri |> Some | _ -> None 
  let (|RevisionUrl|) (r : Revision) =
    let (Url u), revision = r
    Uri(sprintf "%s/%d" u revision)
  let (|ContinuantUrl|) (c : Continuant) = let (Url u) = c in Uri(u)
  let inline revision (u : Uri) = continuant u.FirstPart, UInt16.Parse u.LastPart

  open System.Net.NetworkInformation
  //TODO: use EC2 instance name
  let localHostName =
    let p = IPGlobalProperties.GetIPGlobalProperties()
    urn <| sprintf "urn:dev:relayfoods:machines:%s" p.HostName

  let utcTimestamp () =
    let (Urn u) = localHostName
    Utc (DateTime.UtcNow, u)

  let activity activitiesUrn boundedContext aggregate intent : Activity =
    urn <| sprintf "%s:%s:%s:%s" activitiesUrn boundedContext aggregate intent

  open Intents
  /// The 'i is the result of the domain implementation successfully parsing the
  /// inbound message (Intent)
  type ParsedIntent<'i> = {
    Intent : Intent
    Parsed : 'i }

  open System.Threading
  /// The Entity record is produced by the EventStore. In addition to the current
  /// state projection 's and event history, it carries with it the appropriate
  /// identifiers for the entity. There is a SemaphoreSlim associated with every
  /// entity in order to synchronize access to its timeline (event history).
  /// For example, appending to the timeline or reading the latest value of
  /// the same should be synchronized.
  type Entity<'s, 'e> = {
    Id      : Continuant
    Events  : Event<'s,'e> seq
    State   : 's
    Lock    : SemaphoreSlim // ideally this type would be private--exposing this is a Bad Thing
    Creator : Instigator }
  with
    member x.Revision   = let last = Seq.last x.Events in (x.Id, last.Revision)
    member x.RevisionId = let (RevisionUrl r) = x.Revision in r

  type EventStore<'s, 'e> = {
    Get   : EntityId      -> Entity<'s, 'e> option
    Store : Event<'s,'e>  -> unit
    Apply : Entity<'s,'e> -> Event<'s,'e> -> Entity<'s,'e>
    FindByActivity : Activity -> Limit option -> (Revision*Timestamp) seq
    FindByInstigatorActivity : Instigator -> Activity -> Limit option -> (Revision*Timestamp) seq }
  and Limit = byte
  and EntityId =
  | Current of Continuant
  | Version of Revision

  let (|CurrentId|VersionId|InvalidEntityId|) (u : Uri) =
    match u.AbsoluteUri with
    | IsUrl url ->
      let path (u : Uri) = Uri u.FirstPart
      let continuant (u : Uri) g = tryParse g |> Option.map (fun (_ : Guid) -> Url u.AbsoluteUri)
      let revision u r =
        tryParse r |> Option.map (fun rev -> let p = path u in continuant p p.LastPart
                                             |> Option.map (fun c -> EntityId.Version (c, rev)))
      match revision url url.LastPart with
      | Some (Some r) -> VersionId r
      | _ ->
      match continuant u u.LastPart with
      | Some r -> CurrentId (Current r)
      | _ -> InvalidEntityId
    | _ -> InvalidEntityId

  /// Factory function for creating EventStore instances using functions supplied by
  /// the concrete store implementation
  let eventStore storeDomainEvent getDomainEvents findByActivity findByInstigatorActivity limit apply =
    (*
    Note on "caching": the mechanism for distribution of a Hyperobject (scale out) is
    to eliminate the need for coordination among nodes by ensuring that all requests
    for the same entity are routed to the same node. (see consistent hashing strategies for e.g. HAProxy)
    In fact, the node serving the request is considered the system of record for
    the current state of the entity. The events are durably stored before the
    in-memory representation is updated, but the interpretation of the event
    stream is the province of the Hyperobject node alone. The event store itself
    defines the absolute durability of an entity, so you could have an e.g. in-memory store (very useful for testing).

    Therefore it's not appropriate to think of this as a cache so much as a memory manager.
    A least-recently used cache is an appropriate mechanism to control which entities
    are loaded at any given time. The ASP.NET Web.HttpRuntime.Cache is an LRU cache.
    When an entity's history is loaded into memory for the first time, a consistent
    read should be performed against the event store.
    *)

    /// Stores an entity's timeline in memory along with the lock used to
    /// coordinate access to the same.
    let setCache (key : Guid) (history : Event<'s,'e> list) (l : SemaphoreSlim) =
      Web.HttpRuntime.Cache.[string key] <- (history, l)

    /// Gets the up-to-date (see note on caching above) entity timeline and
    /// the lock used to coordinate access to the same.
    let getCache (key : Guid) : (Event<'s,'e> list option * SemaphoreSlim) =
      match Web.HttpRuntime.Cache.[string key] with
      | :? (Event<'s,'e> list * SemaphoreSlim) as t -> let (e,l) = t in Some e, l
      | _ -> None, new SemaphoreSlim(initialCount = 1, maxCount = 1) // only one thread can access at a time

    /// Gets the current, up-to-date timeline of an entity
    let history (id : Continuant) =
      let (LocalId cacheKey) = id
      match getCache cacheKey with
      | Some e, l -> Some e, l
      | None, l ->
      match getDomainEvents id with
      | [||] -> None, l
      | d ->
        let d' = d |> Array.toList
                   |> List.sortBy (fun e -> e.Revision)
        setCache cacheKey d' l
        Some d', l

    /// Applies domain-defined operations appropriately
    let apply' (r : Entity<'s,'e>) e =
      let s = 
        match e.Operation with
        | ForkOf (_, Some op) -> apply r.State op
        | Domain op -> apply r.State op
        | _ -> r.State
      { r with State = s 
               Events = seq { yield! r.Events; yield e } }

    /// Initialize an Entity record
    let mkEntity (id : Continuant) state lock creator =
      { Id = id; State = state; Events = Seq.empty; Lock = lock; Creator = creator }

    let rec project id lock (h : Event<'s,'e> seq) : Entity<'s,'e> =
      let incipient = h |> Seq.head
      let mk state = mkEntity id state lock incipient.Instigator
      let initial : Entity<'s,'e> =
        match incipient.Operation with
        | ForkOf (parent, _) ->
          let parent = parent ||> getRevision |> Option.get
          mk parent.State
        | Created baseline ->
          mk baseline
        | Originated {Originator = _; Antecedent = _; Initial = s} ->
          mk s
        | badOp ->
          let (Url u) = id
          failwithf "Invalid incipient operation %O for %s" badOp u
      h |> Seq.fold apply' initial
    and get id : Entity<'s,'e> option =
      history id |> getFromHistory id
    and getFromHistory id (h,l) =
      h |> (Option.map List.toSeq)
      |> Option.map (project id l)
    and getRevision id r : Entity<'s,'e> option =
      let (h,l) = history id
      h
      |> Option.map (Seq.takeWhile (fun e -> e.Revision <= r))
      |> Option.map (project id l)
    let get' = function
    | Current c -> get c
    | Version r -> r ||> getRevision

    let store evt =
      // fetch event history; we'll use it first to
      // ensure the proposed evt is applicable and
      // then to store the evt
      let evtHistory = history evt.EntityId

      // sanity check: ensure the event can be applied
      match getFromHistory evt.EntityId evtHistory with
      | Some root ->
        match evt.Operation with
        | Domain op -> apply root.State op |> ignore
        | Created _ | ForkOf _ | Originated _ -> // attempting to apply these to an existing entity
          failwithf "A entity can only be created once %s"
          <| let (Url c) = evt.EntityId in c
        | Archived | ForkedBy _-> ()
      | None -> ()

      // store the event and update the cache
      let (LocalId cacheKey) = evt.EntityId
      match evtHistory with
      | (Some h, l) ->
        let last = h |> Seq.last
        let evt' = { evt with Revision = last.Revision + 1us }
        match storeDomainEvent evt' with
        | Success _ ->
          setCache cacheKey
          <| List.append (fst >> Option.get <| getCache cacheKey) [evt']
          <| l
        | Failure msg -> failwithf "Failed to store event %s" msg
      | (None, l) ->
      match storeDomainEvent evt with
      | Success _ -> setCache cacheKey [evt] l
      | Failure msg -> failwithf "Failed to store event %s" msg

    let find (a: Activity) (Default limit l) =
      let m r =
        let (id, rev, t : Timestamp) = r
        (id, rev), t
      findByActivity a l |> Seq.map m
    let findByInstigatorActivity (User instigator) (activity : Activity) (Default limit _) =
      let m r =
        let (id, rev, t : Timestamp) = r
        (id, rev), t
      findByInstigatorActivity instigator activity
      |> Seq.map m

    { Get = get'
      Store = store
      Apply = apply'
      FindByActivity = find
      FindByInstigatorActivity = findByInstigatorActivity }
