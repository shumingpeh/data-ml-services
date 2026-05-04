from typing import List

from hip_vector_store.growlithe.core import config
from hip_vector_store.growlithe.registry._growlithe_registry_client import (
    _GrowlitheRegistry,
)
from hip_vector_store.growlithe.vector_store._growlithe_vector_store_client import (
    _GrowlitheVectorStore,
)
from loguru import logger


class GrowlitheClient:
    """
    The client is used to manage the databricks vector store, which currently includes
    creation of endpoint and index, and (fast) retrieval of context.

    At the moment, only databricks delta live table is supported.
    The connection and setup is done via the databricks PAT (linked to SPs/users)

    TODO: vector store will need to do some test auth for databricks for init
    TODO: registry will need to do some test auth for databricks for init
    TODO: need to follow the contextual retrieval as how anthropic has done
    """

    def __init__(
        self,
        settings_config: config.Settings,
        vs_endpoint_name: str = None,
        vs_index_name: str = None,
    ):
        """
        Initialise growlithe client

        Parameters
        ----------
        vs_endpoint_name: str = None
            name of vector search endpoint
        vs_index_name: str = None
            name of vector search index
        """

        self.settings_config = settings_config

        self.vector_store_client = None
        if self.settings_config.DATABRICKS_CLUSTER_HOST not in ("", None, "test"):
            self.vector_store_client = _GrowlitheVectorStore(
                settings_config=self.settings_config,
            )
        self.registry = None
        if self.settings_config.DATABRICKS_CLUSTER_HOST not in ("", None, "test"):
            self.registry = _GrowlitheRegistry(settings_config=self.settings_config)

    def change_source_table_format(
        self,
        table_name: str,
        column_name_set_not_null: str = None,
        column_name_primary_key: str = None,
    ):
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
        self.registry._convert_source_table_format(
            table_name=table_name,
            column_name_set_not_null=column_name_set_not_null,
            column_name_primary_key=column_name_primary_key,
        )

    def create_vectorsearch_endpoint_index(
        self,
        vs_endpoint_name: str,
        vs_index_name: str,
        source_table_name: str,
        primary_key: str,
        embedding_source_column: str,
        embedding_model_endpoint_name: str,
    ) -> int:
        """
        function to create a vectorsearch endpoint and index
        # TODO:enable delta sync for source table

        Parameters
        ----------
        vs_endpoint_name: str
            name of vector search endpoint
        vs_index_name: str
            name of vector search index
        source_table_name: str
            name of source delta table to be converted to vs index
        primary_key: str
            indicate which column in source table to be primary key
        embedding_source_column: str
            column name in source table that contains text
        embedding_model_endpoint_name: str
            name of model endpoint to embed text

        Returns
        -------
        int
            success returns a non exit functon value
        """
        # create vs endpoint
        if self.vector_store_client._create_vs_endpoint(
            vs_endpoint_name=vs_endpoint_name
        ):
            raise ValueError("error in creating vs endpoint")

        # create vs index
        if self.vector_store_client._create_vs_index_delta_sync(
            vs_endpoint_name=vs_endpoint_name,
            vs_index_name=vs_index_name,
            source_table_name=source_table_name,
            primary_key=primary_key,
            embedding_source_column=embedding_source_column,
            embedding_model_endpoint_name=embedding_model_endpoint_name,
            polling_step=20,
            polling_max_tries=110,
        ):
            raise ValueError("error in creating vs index")

        return 0

    def sync_index(
        self,
        vs_endpoint_name: str,
        vs_index_name: str,
    ) -> int:
        """
        function to (re)sync the vs index with the underlying source table
        TODO: have this as part of the registry so that there is a clear distinction
        of client roles

        Parameters
        ----------
        vs_endpoint_name: str
            name of vector search endpoint
        vs_index_name: str
            name of vector search index

        Returns
        -------
        int
            success returns a non exit functon value
        """
        try:
            _vs_index = self.vector_store_client.vsc.get_index(
                endpoint_name=vs_endpoint_name, index_name=vs_index_name
            )
            _vs_index.sync()
            return 0
        except Exception as e:
            logger.error(e)
            return 1

    def retrieve_similar_context_index(
        self,
        endpoint_name: str,
        vector_index_name: str,
        query_text: str,
        columns: List,
        num_results: int = 1,
        score_threshold: float = 0.8,
        query_type: str = "HYBRID",
    ) -> List:
        """
        wrapper function to retrieve similar contexts from vector index

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

        return self.registry._retrieve_based_on_similarity(
            endpoint_name=endpoint_name,
            vector_index_name=vector_index_name,
            query_text=query_text,
            columns=columns,
            num_results=num_results,
            score_threshold=score_threshold,
            query_type=query_type,
        )
