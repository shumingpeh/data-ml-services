import polling
from databricks.vector_search.client import VectorSearchClient
from hip_vector_store.growlithe.core import config
from loguru import logger
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_fixed


class _GrowlitheVectorStore:
    """
    The vector store client is used to manage vector search endpoints
    """

    def __init__(
        self,
        settings_config: config.Settings,
        vs_endpoint_name: str = None,
        vs_index_name: str = None,
    ):
        """
        Initialise growlithe vector store client

        Parameters
        ----------
        settings_config: config.Settings
            settings config
        vs_endpoint_name: str = None
            name of vector search endpoint
        vs_index_name: str = None
            name of vector search index
        """

        self.settings_config = settings_config
        self.vs_endpoint_name = vs_endpoint_name
        self.vsc = VectorSearchClient(
            workspace_url=self.settings_config.DATABRICKS_CLUSTER_HOST,
            personal_access_token=self.settings_config.DATABRICKS_PAT_TOKEN,
            disable_notice=True,
        )
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

    def _check_vs_endpoint_exists(self, vs_endpoint_name: str) -> int:
        """
        function to check if name of vs endpoint exists

        Parameters
        ----------
        vs_endpoint_name: str
            name of vector search endpoint

        Returns
        ----------
        int
            success returns a non exit functon value
        """
        try:
            if vs_endpoint_name in [
                endpoint["name"]
                for endpoint in self.vsc.list_endpoints().get("endpoints", [])
            ]:
                return 0
            return 1
        except Exception as e:
            logger.error(e)
            return 1

    def _get_endpoint_state_status(self, endpoint, type_of_creation: str) -> str:
        """
        function to retrieve the endpoint state status

        Parameters
        ----------
        endpoint: str
            name of vector search endpoint/index
        type_of_creation: str
            type of creation; index or endpoint

        Returns
        ----------
        int
            success returns a non exit functon value
        """
        try:
            if type_of_creation == "endpoint":
                return endpoint.get("endpoint_status", endpoint.get("status"))[
                    "state"
                ].upper()

            return endpoint.get("status").get("detailed_state", "UNKNOWN").upper()
        except Exception:
            return "NOT_READY"

    def _create_vs_endpoint(
        self,
        vs_endpoint_name: str,
        polling_step: int = 20,
        polling_max_tries: int = 90,
    ) -> int:
        """
        function to create vector search endpoint

        Parameters
        ----------
        vs_endpoint_name: str
            name of vector search endpoint
        polling_step: int = 20
            polling interval
        polling_max_tries: int = 90
            maximum number of tries for polling

        Returns
        ----------
        int
            success returns a non exit functon value

        """
        # check if endpoint exists, if not create endpoint
        if self._check_vs_endpoint_exists(vs_endpoint_name=vs_endpoint_name):
            logger.info(f"creating vector search endpoint: {vs_endpoint_name}")
            self.vsc.create_endpoint(
                name=vs_endpoint_name,
                endpoint_type=self.settings_config.VS_ENDPOINT_TYPE,
            )

            # poll to check if endpoint is up and running
            polling_response = polling.poll(
                lambda: self._get_endpoint_state_status(
                    endpoint=self.vsc.get_endpoint(vs_endpoint_name),
                    type_of_creation="endpoint",
                )
                in "ONLINE",
                step=polling_step,
                poll_forever=False,
                max_tries=polling_max_tries,
            )

            if not polling_response:
                polling_response.raise_for_status()

            logger.info(f"finish creating vector search endpoint: {vs_endpoint_name}")
            return 0

        logger.info(f"vector search endpoint: {vs_endpoint_name} alr exists")
        return 0

    def _check_vs_index_exists(self, vs_endpoint_name: str, vs_index_name: str) -> int:
        """
        function to check if name of vs index exists

        Parameters
        ----------
        vs_endpoint_name: str
            name of vs endpoint
        vs_index_name: str
            name of vector search index

        Returns
        ----------
        int
            success returns a non exit functon value
        """
        try:
            if vs_index_name in [
                index["name"]
                for index in self.vsc.list_indexes(name=vs_endpoint_name).get(
                    "vector_indexes", []
                )
            ]:
                return 0
            return 1
        except Exception as e:
            logger.error(e)
            return 1

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
    def _create_vs_index_delta_sync(
        self,
        vs_endpoint_name: str,
        vs_index_name: str,
        source_table_name: str,
        primary_key: str,
        embedding_source_column: str,
        embedding_model_endpoint_name: str,
        polling_step: int = 20,
        polling_max_tries: int = 90,
    ) -> int:
        """
        function to create vector search index (delta sync)

        Parameters
        ----------
        vs_endpoint_name: str
            name of vs endpoint
        vs_index_name: str
            name of vector search index
        source_table_name: str
            name of the lakehouse delta table
        primary_key: str
            name of column from `source_table_name` to be primary key
        embedding_source_column: str
            name of column from `source_table_name` to be referenced as embedding source
        embedding_model_endpoint_name: str
            name of model endpoint name that can embed the text to vectors
        polling_step: int = 20
            polling interval
        polling_max_tries: int = 90
            maximum number of tries for polling

        Returns
        ----------
        int
            success returns a non exit functon value
        """
        # check if endpoint exists, if not create endpoint
        if self._check_vs_index_exists(
            vs_endpoint_name=vs_endpoint_name, vs_index_name=vs_index_name
        ):
            logger.info(
                f"Creating index, {vs_index_name}, on endpoint {vs_endpoint_name}"
            )

            self.vsc.create_delta_sync_index(
                endpoint_name=vs_endpoint_name,
                index_name=vs_index_name,
                source_table_name=source_table_name,
                pipeline_type=self.settings_config.PIPELINE_TYPE,
                primary_key=primary_key,
                embedding_source_column=embedding_source_column,
                embedding_model_endpoint_name=embedding_model_endpoint_name,
            )

            # poll to check if the index is up and running
            # idx = vsc.get_index(vs_endpoint_name, index_name).describe()
            polling_response = polling.poll(
                lambda: "ONLINE"
                in self._get_endpoint_state_status(
                    endpoint=self.vsc.get_index(
                        vs_endpoint_name, vs_index_name
                    ).describe(),
                    type_of_creation="index",
                ),
                step=polling_step,
                poll_forever=False,
                max_tries=polling_max_tries,
            )

            if not polling_response:
                polling_response.raise_for_status()

            logger.info(f"finish creating vector search index: {vs_index_name}")
            return 0

        logger.info(f"vector search index: {vs_index_name} alr exists")
        return 0
