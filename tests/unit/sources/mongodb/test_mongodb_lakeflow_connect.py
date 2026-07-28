"""Tests for the MongoDB connector.

This connector talks to MongoDB through the PyMongo driver (not HTTP), so
it has no offline simulator. Run the generic suite in live mode against a
real MongoDB / Atlas cluster by supplying credentials via
``CONNECTOR_TEST_CONFIG_JSON`` or ``CONNECTOR_TEST_CONFIG_PATH``::

    CONNECTOR_TEST_MODE=live \\
      CONNECTOR_TEST_CONFIG_JSON='{"connection_uri": "mongodb+srv://...", "database": "mydb"}' \\
      pytest tests/unit/sources/mongodb/ -v
"""

from databricks.labs.community_connector.sources.mongodb.mongodb import (
    MongoDBLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMongoDBConnector(LakeflowConnectTests):
    connector_class = MongoDBLakeflowConnect
    # No simulator: credentials must be supplied per run via
    # CONNECTOR_TEST_CONFIG_JSON / CONNECTOR_TEST_CONFIG_PATH (live mode).
