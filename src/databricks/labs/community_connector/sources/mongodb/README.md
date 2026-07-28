# Lakeflow MongoDB Community Connector

This documentation provides setup instructions and reference information for the MongoDB source connector.

The Lakeflow MongoDB Connector extracts data from MongoDB (including MongoDB Atlas) collections and loads it into Databricks. It supports **snapshot** (full-refresh) ingestion and, per collection, **CDC** (incremental) ingestion driven by a monotonic cursor field.

## Features

- **Automatic collection discovery**: Every user collection in the configured database is exposed as an ingestible table.
- **Schema-stable envelope**: Each document is emitted as `_id` and `document_json`, so heterogeneous collections keep a stable Spark schema.
- **BSON preservation**: Documents are serialised as MongoDB Extended JSON (Relaxed mode), preserving BSON types such as `ObjectId`, `Decimal128`, dates and binary data.
- **Snapshot or incremental (CDC)**: Choose per collection. Snapshot is the default; setting `cursor_field` switches a collection to incremental reads.
- **Atlas ready**: Uses the official PyMongo driver with SRV connection strings.

## Prerequisites

- A MongoDB deployment (self-managed or MongoDB Atlas).
- A database user with read access to the target database.
- Network access from Databricks to the MongoDB cluster. For Atlas, add the appropriate IP access list entry (or private endpoint) so Databricks can connect.

## Setup

### Connection Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `connection_uri` | string (secret) | Yes | MongoDB connection string. For Atlas, the SRV string from the Atlas UI. | `mongodb+srv://user:pass@cluster.mongodb.net` |
| `database` | string | Yes | Database to read collections from. | `sample_mflix` |

### Table Options

| Option | Required | Description | Default |
|--------|----------|-------------|---------|
| `batch_size` | No | Number of documents PyMongo fetches per network round-trip. `0` uses the driver default. | `0` |
| `cursor_field` | No | Top-level document field used as the incremental cursor. When set, the collection is ingested in **CDC** mode; otherwise it is a snapshot. | (unset) |
| `cursor_type` | No | Type of the cursor field: `timestamp` (BSON date) or `objectid` (`ObjectId`, e.g. `_id`). | `timestamp` |
| `max_records_per_batch` | No | Maximum documents returned per incremental read (microbatch size). CDC only. | `1000` |
| `start_timestamp` | No | Lower bound for the first CDC read when no offset exists yet. For `timestamp` cursors use an ISO-8601 value; for `objectid` cursors an `ObjectId` hex string. | (unset) |

### How to Obtain the Connection String

For MongoDB Atlas:

1. In the Atlas UI, open your cluster and click **Connect**.
2. Choose **Drivers**.
3. Copy the connection string (starts with `mongodb+srv://`).
4. Replace `<username>` and `<password>` with a database user's credentials.

## Data Model

Each collection produces rows with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `_id` | string | The document's `_id` rendered as a stable string. |
| `document_json` | string | The full document as MongoDB Extended JSON (Relaxed mode). |
| `<cursor_field>` | timestamp or string | CDC only: the cursor field surfaced as its own typed column (`timestamp` for `cursor_type=timestamp`, `string` for `objectid`). Omitted when the cursor is `_id` itself. |

The primary key is always `_id`.

To work with individual document fields downstream, parse `document_json` in Databricks (for example with `from_json` / `parse_json` / `:` accessors).

## Ingestion Modes

### Snapshot (default)

Without `cursor_field`, the ingestion type is `snapshot`: the full collection is read on every trigger and the destination table is fully refreshed.

### CDC (incremental)

Set `cursor_field` (and `cursor_type` if the field is not a timestamp) to ingest a collection incrementally. Each trigger reads only documents whose cursor value is greater than the last checkpoint, ordered ascending, in microbatches of `max_records_per_batch`. Rows are upserted by `_id`.

Requirements and behaviour:

- **Monotonic cursor**: the field must only increase over time (e.g. an `updated_at` timestamp bumped on every write, or an `ObjectId` such as `_id`). A non-monotonic field will skip data.
- **Index required**: create an index on the cursor field. The connector sorts by it, and MongoDB aborts in-memory sorts larger than 32 MB without an index.
- **Cursor type must match the BSON type** stored in the documents. A `timestamp` cursor will not match a field stored as a string.
- **Deletes are not tracked** in this version.
- Documents that lack the cursor field are not ingested in CDC mode.
- Data written after a trigger starts is picked up by the next trigger.

## Limitations

- No delete tracking (no `cdc_with_deletes` / change streams yet).
- Snapshot mode reads the full collection on every trigger.
- Nested fields are not flattened into columns; they remain inside `document_json`.
