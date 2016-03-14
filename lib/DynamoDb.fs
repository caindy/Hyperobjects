// #r AWSSDK
open System
open System.Collections.Generic
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.DynamoDBv2.DocumentModel

module DynamoDb =
  let client = new AmazonDynamoDBClient();

  let rec tables lastTable = seq {
    let listTablesRequest =
      new ListTablesRequest(Limit = 10, ExclusiveStartTableName = lastTable)
    let response = client.ListTables(listTablesRequest)
    yield! response.TableNames
    let t = response.LastEvaluatedTableName
    if not (isEmpty t) then yield! tables t }

  let existingTables () =
    tables null |> Seq.map (fun t -> sprintf "table name %s" t)

  let keySchema (hash : AttributeDefinition) (range : AttributeDefinition) =
    List [ KeySchemaElement(hash.AttributeName, KeyType.HASH)
           KeySchemaElement(range.AttributeName, KeyType.RANGE) ]

  let createDomainEventsTable tableName =
    match Table.TryLoadTable(client, tableName) with
    | true, table -> table
    | _ ->
      let continuant    = AttributeDefinition("id", ScalarAttributeType.S)
      let timestamp     = AttributeDefinition("timestamp", ScalarAttributeType.S)
      let activityName  = AttributeDefinition("activity", ScalarAttributeType.S)
      let revision      = AttributeDefinition("revision", ScalarAttributeType.N)
      let instigator    = AttributeDefinition("instigator", ScalarAttributeType.S)
      let attributes    = List [ continuant; timestamp; revision; instigator; activityName ]
      let throughput    = ProvisionedThroughput(int64 1, int64 1)
      let idxthroughput = ProvisionedThroughput(int64 1, int64 1)

      let userQueryIndex =
        GlobalSecondaryIndex(IndexName = "UserQueryIndex",
                             KeySchema = keySchema instigator activityName,
                             Projection = Projection(ProjectionType = ProjectionType.INCLUDE,
                                                     NonKeyAttributes = List [revision.AttributeName]),
                             ProvisionedThroughput = idxthroughput)
      let activityQueryIdx =
        GlobalSecondaryIndex(IndexName = "ActivityQueryIndex",
                             KeySchema = keySchema activityName timestamp,
                             Projection = Projection(ProjectionType = ProjectionType.INCLUDE,
                                                     NonKeyAttributes = List [instigator.AttributeName; revision.AttributeName]),
                             ProvisionedThroughput = idxthroughput)

      let request = new CreateTableRequest(tableName, keySchema continuant revision, attributes, throughput)
      request.GlobalSecondaryIndexes <- List [userQueryIndex; activityQueryIdx]
      let streams = StreamSpecification(StreamEnabled = true, StreamViewType = StreamViewType.NEW_IMAGE)
      request.StreamSpecification <- streams

      let result = client.CreateTable(request)
      match result.HttpStatusCode with
      | Net.HttpStatusCode.Created | Net.HttpStatusCode.OK | Net.HttpStatusCode.Accepted  ->
        let rec poll () =
          let describeResult = client.DescribeTable(tableName)
          match describeResult.Table.TableStatus with
          | status when status = TableStatus.ACTIVE -> Table.LoadTable(client, tableName)
          | _ -> poll ()
        poll ()
      | _ -> failwithf "Could not create table %s" tableName

  open System.Globalization
  let dtFormat = CultureInfo.InvariantCulture.DateTimeFormat
  let formatTimestamp (t : MachineClock) =
    let (Utc (t, n)) = t
    sprintf "%s %s" (t.ToString("o", dtFormat)) n
  let parseTimestamp (s : string) =
    match s.Split(' ') |> Array.map (fun s -> s.Trim()) with
    | [|t;n|] ->
      let utc = DateTime.ParseExact(t, "o", dtFormat, DateTimeStyles.AdjustToUniversal)
      Utc (utc, n)
    | _ -> failwithf "%s is not a valid timestamp" s

  // open EventPersist should implement serialize/deserialize
  open Relay.Hyperobjects.Aggregates
  let rec storeDomainEvent (table: Table) (e: Event<'s,'e>) =
    let event = Document()
    let dbentry o = DynamoDBEntryConversion.V2.ConvertToEntry(o)
    let add s o = event.[s] <- dbentry o
    let addJson s o = event.Add(s, Document.FromJson o)

    add "activity" <| string e.Activity
    addJson "operation" (serialize e.Operation)
    let timestamp = formatTimestamp e.Timestamp
    add "timestamp" timestamp
    let (InstigatorUri i) = e.Instigator
    add "instigator" i.AbsoluteUri
    add "revision" e.Revision
    addJson "impetus" (serialize e.Impetus)
    let uri = let (ContinuantUrl url) = e.EntityId in Primitive(url.AbsoluteUri)
    let revision = Primitive(string e.Revision, saveAsNumeric = true)
    try
      let event' = table.UpdateItem(event, uri, revision)
      Success event'
    with ex ->
      Failure (string ex)
     
  let getDomainEvents<'s,'e> (table: Table) (id: Continuant) : Event<'s,'e> array =
    let uri =
      let (ContinuantUrl url) = id in Primitive(url.AbsoluteUri)
    let hashKeyFilter = QueryFilter()
    hashKeyFilter.AddCondition("id", QueryOperator.Equal, uri);
    let config = QueryOperationConfig(ConsistentRead = true, Filter = hashKeyFilter)
    let search = table.Query(config)
    let timestamp (doc: Document) =
      let s = doc.["timestamp"].AsString()
      parseTimestamp s
    search.GetRemaining()
    |> Array.ofSeq
    |> Array.map (fun doc ->
      let (Instigator i) = doc.["instigator"].AsString()
      { EntityId = id
        Activity = doc.["activity"].AsString() |> urn
        Operation = doc.["operation"].AsDocument().ToJson() |> deserialize
        Impetus = doc.["impetus"].AsDocument().ToJson() |> deserialize
        Timestamp = timestamp doc
        Instigator = i
        Revision = doc.["revision"].AsUShort() })

  let activityQuery (table: Table) (activity: Activity) (instigator : Uri option) (limit : byte option) (pageSize : byte option) =
    let query = sprintf "SELECT id, revision, timestamp FROM %s WHERE activity = \"%A\" WITH(%s,NoConsistentRead) ORDER DESC %s"
                  (table.TableName) activity
    let index =
      match instigator with
      | Some _ -> "INDEX(UserQueryIndex, false)"
      | _      -> "INDEX(ActivityQueryIndex,false)"
    let limit' =
      match limit, pageSize with
      | Some l, None -> sprintf "LIMIT %d" l
      | _ -> "(PageSize(50))"
    let response = client.Query(query index limit')
    response.Items
    |> Seq.map (fun r ->
                  continuant r.["id"].S,
                  (uint16 r.["revision"].N),
                  Simple <| Machine (parseTimestamp r.["timestamp"].S))

