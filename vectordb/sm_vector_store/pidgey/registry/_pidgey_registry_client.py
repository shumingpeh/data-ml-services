from typing import List

from databricks.vector_search.client import VectorSearchClient
from databricks.vector_search.reranker import DatabricksReranker
from loguru import logger
from sm_data_ml_utils.databricks_client.client import DatabricksSQLClient
from sm_vector_store.pidgey.core import config


class _PidgeyRegistry:
    """
    The registry client is mainly a wrapper for the databricks vector search
    python library. Here, we mainly deal with the retrieval

    TODO: add contextual embeddings to the original delta lake table
    """

    def __init__(self, settings_config: config.Settings):
        """
        Initialise pidgey registry client

        Parameters
        ----------
        settings_config: config.Settings
            settings config
        """

        self.settings_config = settings_config
        self.vsc = VectorSearchClient(
            workspace_url=self.settings_config.DATABRICKS_CLUSTER_HOST,
            personal_access_token=self.settings_config.DATABRICKS_TOKEN,
            disable_notice=True,
        )
        self.databricks_client = DatabricksSQLClient()

        # test databricks connection
        if not self._test_connection_databricks():
            raise ValueError("Databricks creds provided are incorrect")

    def _test_connection_databricks(self) -> bool:
        """
        function to test connection to databricks

        Returns
        ----------
        bool
            if the test connection is successful or not
        """
        try:
            self.vsc.list_endpoints()
            return True
        except Exception as e:
            logger.exception(e)
            return False

    def _retrieve_based_on_similarity(
        self,
        endpoint_name: str,
        vector_index_name: str,
        query_text: str,
        columns: List,
        columns_rerank: List,
        num_results: int = 1,
        score_threshold: float = 0.8,
        query_type: str = "HYBRID",
    ) -> List:
        """
        function to retrieve similar contexts from vector index

        Parameters
        ----------
        endpoint_name: str
            vs endpoint name
        vector_index_name: str
            vs index name
        query_text: str
            query text to compare with vector index
        columns: List
            list of column to return from vector index
        num_results: int = 1
            number of results to return, default at 1
        score_threshold: float = 0.8
            similarity score threshold to return value, default at 0.8
        query_type: str = "HYBRID"
            similarity query type, hybrid or ann; default at hybrid
            hybrid includes HNSW + bm25. databricks did not disclose the weight

        Returns
        ----------
        List
            retrieved information stored in list. possible that 0 results returned
        """
        vs_index = self.vsc.get_index(
            endpoint_name=endpoint_name, index_name=vector_index_name
        )

        results = vs_index.similarity_search(
            query_text=query_text,
            columns=columns,
            query_type=query_type,
            score_threshold=score_threshold,
            num_results=num_results,
            disable_notice=True,
            reranker=DatabricksReranker(columns_to_rerank=columns_rerank),
        )

        data_array = results.get("result", {}).get("data_array", [])

        return [chunk for chunk in data_array if chunk[4] >= score_threshold]

    def _convert_source_table_format(
        self,
        table_name: str,
        column_name_set_not_null: str = None,
        column_name_primary_key: str = None,
    ) -> None:
        """
        function to convert delta table to enable continuous or triggered sync

        Parameters
        ----------
        table_name: str
            table name to be formatted
        column_name_set_not_null: str = None
            column name from table to be set as not null
        column_name_primary_key: str = None
            column name from table to be set as primary key

        Returns
        ----------
        None
            no return type after execution
        """
        try:
            self.databricks_client.query_as_pandas(
                final_query=f"""ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true);"""  # noqa: E501
            )
            if column_name_set_not_null is not None:
                self.databricks_client.query_as_pandas(
                    final_query=f"""ALTER TABLE {table_name} ALTER COLUMN {column_name_set_not_null} SET NOT NULL;"""  # noqa: E501
                )
            if column_name_primary_key is not None:
                self.databricks_client.query_as_pandas(
                    final_query=f"""ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name_primary_key});"""  # noqa: E501
                )
        except Exception as e:
            logger.error(e)
