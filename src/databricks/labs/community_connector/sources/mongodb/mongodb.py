"""MongoDB (Atlas) connector for Lakeflow Community Connectors.

Each document is emitted through a stable envelope schema:

- ``_id``: the document identifier rendered as a stable string.
- ``document_json``: the full document serialised as MongoDB Extended
  JSON (Relaxed mode), which preserves BSON types such as ObjectId,
  Decimal128, dates and binary data.

This keeps the Spark schema stable regardless of how heterogeneous the
documents in a collection are, which is the common case in MongoDB.

Two ingestion modes are supported, selected per collection:

- **snapshot** (default): the whole collection is read on every trigger.
- **cdc**: incremental reads driven by a monotonic cursor field. Enable
  it per collection by setting the ``cursor_field`` table option. When
  set, an extra typed column named after the cursor field is added to the
  schema so the framework can use it as the sequencing key. The cursor
  field type is declared via ``cursor_type`` (``timestamp`` or
  ``objectid``; defaults to ``timestamp``).

Deletes are not tracked in this version.
"""

from datetime import datetime, timezone
from typing import Iterator, Optional

from bson import ObjectId
from bson.json_util import RELAXED_JSON_OPTIONS, dumps
from pymongo import ASCENDING, MongoClient
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect

# Relaxed Extended JSON keeps numbers/dates human-readable while still
# round-tripping the BSON types MongoDB documents can contain.
_JSON_OPTIONS = RELAXED_JSON_OPTIONS

# MongoDB stores internal collections under the ``system.*`` prefix; they
# are not user data and must not be surfaced as ingestible tables.
_SYSTEM_COLLECTION_PREFIX = "system."

# Connection timeouts (milliseconds). Always bounded so a misconfigured
# URI or unreachable cluster fails fast instead of hanging the pipeline.
_SERVER_SELECTION_TIMEOUT_MS = 20_000
_CONNECT_TIMEOUT_MS = 20_000

# Default cap on documents returned per read_table call in CDC mode.
_DEFAULT_MAX_RECORDS_PER_BATCH = 1000

_CURSOR_TYPE_TIMESTAMP = "timestamp"
_CURSOR_TYPE_OBJECTID = "objectid"
_VALID_CURSOR_TYPES = (_CURSOR_TYPE_TIMESTAMP, _CURSOR_TYPE_OBJECTID)

_ID_FIELD = "_id"


class MongoDBLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for MongoDB / MongoDB Atlas."""

    def __init__(self, options: dict[str, str]) -> None:
        """Initialise the connector.

        Args:
            options: Connection parameters:
                - connection_uri: MongoDB connection string, e.g.
                  ``mongodb+srv://user:pass@cluster.mongodb.net`` (required).
                - database: Name of the database to read from (required).

        Raises:
            ValueError: If a required parameter is missing.
        """
        super().__init__(options)

        self._connection_uri = options.get("connection_uri")
        if not self._connection_uri:
            raise ValueError("Missing required parameter 'connection_uri'")

        self._database = options.get("database")
        if not self._database:
            raise ValueError("Missing required parameter 'database'")

        # Cap the cursor at init time so a single trigger only drains data
        # that existed when the connector started. The next trigger builds
        # a fresh instance with a newer cap and picks up the rest, which is
        # what makes CDC reads converge under Trigger.AvailableNow.
        self._init_dt = datetime.now(timezone.utc)

    def _client(self) -> MongoClient:
        """Create a new MongoClient.

        A fresh client is created per call rather than cached on the
        instance: Spark serialises the connector to ship it to executors,
        and a live ``MongoClient`` (which owns background threads and
        sockets) is not picklable. Callers own the returned client's
        lifecycle and must close it.
        """
        return MongoClient(
            self._connection_uri,
            serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=_CONNECT_TIMEOUT_MS,
        )

    def list_tables(self) -> list[str]:
        """Return all user collections in the configured database."""
        client = self._client()
        try:
            db = client[self._database]
            return [
                name
                for name in db.list_collection_names()
                if not name.startswith(_SYSTEM_COLLECTION_PREFIX)
            ]
        finally:
            client.close()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Return the envelope schema, plus a cursor column in CDC mode.

        Snapshot tables expose ``_id`` + ``document_json``. CDC tables add
        a typed column named after ``cursor_field`` (unless the cursor is
        ``_id`` itself, which already exists as a column).
        """
        self._validate_table(table_name)
        fields = [
            StructField(_ID_FIELD, StringType(), False),
            StructField("document_json", StringType(), False),
        ]
        cursor = self._resolve_cursor(table_options)
        if cursor and cursor[0] != _ID_FIELD:
            cursor_field, cursor_type = cursor
            col_type = TimestampType() if cursor_type == _CURSOR_TYPE_TIMESTAMP else StringType()
            fields.append(StructField(cursor_field, col_type, True))
        return StructType(fields)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return snapshot metadata, or CDC metadata when a cursor is set."""
        self._validate_table(table_name)
        cursor = self._resolve_cursor(table_options)
        if cursor is None:
            return {
                "primary_keys": [_ID_FIELD],
                "ingestion_type": "snapshot",
            }
        return {
            "primary_keys": [_ID_FIELD],
            "cursor_field": cursor[0],
            "ingestion_type": "cdc",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read a collection.

        Routes to a snapshot full read (empty offset) or, when
        ``cursor_field`` is configured, to an incremental read that
        advances a checkpointable cursor offset.
        """
        self._validate_table(table_name)
        cursor = self._resolve_cursor(table_options)
        if cursor is None:
            return self._read_snapshot(table_name, table_options), {}
        return self._read_incremental(table_name, start_offset, table_options, cursor)

    def _read_snapshot(self, table_name: str, table_options: dict[str, str]) -> Iterator[dict]:
        """Yield every document in the collection as an envelope record.

        Implemented as a generator so the MongoClient stays open for the
        lifetime of the cursor and is closed once iteration finishes.
        """
        batch_size = self._resolve_batch_size(table_options)

        client = self._client()
        try:
            cursor = client[self._database][table_name].find({})
            if batch_size:
                cursor = cursor.batch_size(batch_size)
            for document in cursor:
                yield self._to_envelope(document, None)
        finally:
            client.close()

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        cursor: tuple[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read one bounded, ordered microbatch newer than ``start_offset``.

        The batch is materialised (capped by ``max_records_per_batch``) so
        the end offset can be derived from the last record before
        returning. The query is bounded above by the init-time cap and
        sorted ascending by the cursor field, which guarantees the offset
        advances and the read converges under Trigger.AvailableNow.
        """
        cursor_field, cursor_type = cursor
        max_records = self._resolve_max_records(table_options)
        batch_size = self._resolve_batch_size(table_options)

        since_str = start_offset.get("cursor") if start_offset else None
        if since_str is None:
            since_str = table_options.get("start_timestamp")

        since_native = (
            self._str_to_native(since_str, cursor_type) if since_str is not None else None
        )
        init_cap = self._init_cap(cursor_type)

        condition: dict = {"$lte": init_cap}
        if since_native is not None:
            condition["$gt"] = since_native
        query = {cursor_field: condition}

        records = []
        last_cursor_value = None
        client = self._client()
        try:
            db_cursor = (
                client[self._database][table_name]
                .find(query)
                .sort(cursor_field, ASCENDING)
                .limit(max_records)
            )
            if batch_size:
                db_cursor = db_cursor.batch_size(batch_size)
            for document in db_cursor:
                records.append(self._to_envelope(document, cursor))
                last_cursor_value = document.get(cursor_field)
        finally:
            client.close()

        if not records:
            return iter([]), (start_offset or {})

        end_offset = {"cursor": self._cursor_to_str(last_cursor_value)}
        if start_offset and end_offset == start_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _resolve_cursor(self, table_options: dict[str, str]) -> Optional[tuple[str, str]]:
        """Return ``(cursor_field, cursor_type)`` for CDC, or ``None`` for snapshot."""
        cursor_field = table_options.get("cursor_field")
        if not cursor_field:
            return None
        cursor_type = (table_options.get("cursor_type") or _CURSOR_TYPE_TIMESTAMP).strip().lower()
        if cursor_type not in _VALID_CURSOR_TYPES:
            raise ValueError(
                f"Invalid 'cursor_type': {cursor_type!r}. "
                f"Must be one of {list(_VALID_CURSOR_TYPES)}."
            )
        return cursor_field, cursor_type

    @staticmethod
    def _resolve_max_records(table_options: dict[str, str]) -> int:
        """Parse the optional ``max_records_per_batch`` table option."""
        raw = table_options.get("max_records_per_batch")
        if raw is None or str(raw).strip() == "":
            return _DEFAULT_MAX_RECORDS_PER_BATCH
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid 'max_records_per_batch' option: {raw!r}") from exc
        if value <= 0:
            raise ValueError(f"'max_records_per_batch' must be positive, got {value}")
        return value

    @staticmethod
    def _resolve_batch_size(table_options: dict[str, str]) -> int:
        """Parse the optional ``batch_size`` table option (0 = driver default)."""
        raw = table_options.get("batch_size")
        if raw is None or str(raw).strip() == "":
            return 0
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid 'batch_size' option: {raw!r}") from exc
        if value < 0:
            raise ValueError(f"'batch_size' must be non-negative, got {value}")
        return value

    def _init_cap(self, cursor_type: str):
        """Return the upper-bound cursor value for this trigger, in native form."""
        if cursor_type == _CURSOR_TYPE_OBJECTID:
            return ObjectId.from_datetime(self._init_dt)
        return self._init_dt

    @staticmethod
    def _str_to_native(value: str, cursor_type: str):
        """Convert a stored/user cursor string back to its native BSON type."""
        if cursor_type == _CURSOR_TYPE_OBJECTID:
            return ObjectId(value)
        normalised = value.replace("Z", "+00:00") if value.endswith("Z") else value
        return datetime.fromisoformat(normalised)

    @staticmethod
    def _cursor_to_str(value) -> str:
        """Render a native cursor value as a stable, JSON-serialisable string."""
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def _to_envelope(self, document: dict, cursor: Optional[tuple[str, str]]) -> dict:
        """Convert a raw BSON document into an envelope record.

        In CDC mode the cursor field is also surfaced as its own typed
        column so the framework can sequence by it.
        """
        if _ID_FIELD not in document:
            raise ValueError("Encountered a document without an '_id' field")
        record = {
            _ID_FIELD: str(document[_ID_FIELD]),
            "document_json": dumps(document, json_options=_JSON_OPTIONS),
        }
        if cursor and cursor[0] != _ID_FIELD:
            cursor_field, cursor_type = cursor
            value = document.get(cursor_field)
            if value is None:
                record[cursor_field] = None
            elif cursor_type == _CURSOR_TYPE_OBJECTID:
                record[cursor_field] = str(value)
            else:
                record[cursor_field] = value
        return record

    def _validate_table(self, table_name: str) -> None:
        supported = self.list_tables()
        if table_name not in supported:
            raise ValueError(f"Collection '{table_name}' not found. Available: {supported}")
