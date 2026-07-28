"""MongoDB source connector."""

from databricks.labs.community_connector.sources.mongodb.mongodb import (
    MongoDBLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class MongoDBDataSource(LakeflowSource):
    _lakeflow_connect_cls = MongoDBLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "mongodb"


__all__ = ["MongoDBLakeflowConnect", "MongoDBDataSource"]
