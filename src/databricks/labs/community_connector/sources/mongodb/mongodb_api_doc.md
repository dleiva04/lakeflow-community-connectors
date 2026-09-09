# MongoDB Source API Notes

Technical reference for the MongoDB connector implementation. MongoDB is
accessed through the official PyMongo driver over the MongoDB wire
protocol, not a REST API.

## Access method

- Driver: `pymongo` (with the `srv` extra for Atlas `mongodb+srv://` URIs).
- Auth: credentials are embedded in the `connection_uri` connection string.
- Client lifecycle: a fresh `MongoClient` is created per operation and
  closed afterwards. Clients are never cached on the connector instance
  because Spark pickles the connector to ship it to executors and a live
  `MongoClient` (background monitoring threads + sockets) is not
  picklable.

## Collection discovery

- `list_tables()` calls `db.list_collection_names()` on the configured
  database and filters out internal `system.*` collections.
- Each remaining collection is treated as one ingestible table.

## Reading

The read path is selected per collection by the presence of the
`cursor_field` table option.

### Snapshot (no `cursor_field`)

- `read_table()` runs `collection.find({})` and streams every document
  via a generator (the client stays open for the cursor's lifetime).
- The optional `batch_size` table option maps to PyMongo's
  `cursor.batch_size(n)` (network round-trip sizing only; `0` = driver
  default).
- The returned offset is always `{}` (no cursor to checkpoint).

### CDC (with `cursor_field`)

- Query: `find({cursor_field: {"$gt": since, "$lte": init_cap}})`,
  `.sort(cursor_field, ASCENDING)`, `.limit(max_records_per_batch)`.
- `since` comes from the previous offset (`start_offset["cursor"]`) or,
  on the first read, from the optional `start_timestamp` option.
- `init_cap` is derived from `datetime.now(UTC)` captured in `__init__`
  (`timestamp` cursors use it directly; `objectid` cursors use
  `ObjectId.from_datetime(init_dt)`). Bounding the query above by the cap
  plus ascending sort guarantees the offset advances and the read
  converges under `Trigger.AvailableNow` (`end_offset == start_offset`
  once drained).
- The microbatch is materialised (bounded by `max_records_per_batch`) so
  the end offset can be taken from the last record before returning.
- Offset shape: `{"cursor": "<value as string>"}` — ISO-8601 for
  timestamps, hex for ObjectId. Converted back to the native BSON type
  when building the next query.
- `cursor_type` (`timestamp` | `objectid`) selects both the parsing of
  the offset and the type of the extra cursor column in the schema.

## Schema and BSON handling

- Envelope schema: `_id STRING`, `document VARIANT`. In CDC mode a
  third column named after `cursor_field` is added (`TIMESTAMP` for
  `cursor_type=timestamp`, `STRING` for `objectid`), unless the cursor is
  `_id` (already present as a column).
- `_id` is rendered with `str()` — for `ObjectId` this yields the 24-char
  hex string.
- The full document is converted to JSON-compatible Python containers and
  stored as VARIANT. MongoDB arrays and nested documents therefore remain
  queryable arrays and objects. BSON-only scalar values use Relaxed Extended
  JSON representations:
  - `ObjectId` -> `{"$oid": "..."}`
  - `Decimal128` -> `{"$numberDecimal": "..."}`
  - dates -> `{"$date": "..."}`
  - binary -> `{"$binary": {...}}`
- The envelope keeps the Spark schema deterministic regardless of
  per-document field/type drift, which is common in MongoDB collections.

## Limitations

- CDC requires a monotonic cursor field with a matching index; the
  `cursor_type` must match the stored BSON type.
- No delete tracking (no `cdc_with_deletes` / change streams yet).
- No nested-field flattening; nested data stays inside the `document` VARIANT.
- Requires a Databricks Runtime with Spark `VariantType` support.
- No server-side projection; snapshot scans the entire collection.
- Documents lacking the cursor field are not ingested in CDC mode.
