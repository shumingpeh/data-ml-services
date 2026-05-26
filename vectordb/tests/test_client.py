from unittest.mock import patch

from sm_vector_store.growlithe.client import GrowlitheClient


@patch("sm_vector_store.growlithe.client._GrowlitheRegistry")  # noqa: E501
@patch("sm_vector_store.growlithe.client._GrowlitheVectorStore")  # noqa: E501
class TestGrowlitheClient:
    """
    test class for growlithe client
    """

    def test_init(self, mock_vector, mock_registry, dummy_settings):
        """
        test init function for growlithe client

        Parameters
        ----------
        mock_vector
            mock databricks vector store client
        mock_registry
            mock databricks vector registry client
        dummy_settings
            dummy settings for databricks vector store client

        Returns
        -------
        assert
            return value is as expected
        """

        client = GrowlitheClient(settings_config=dummy_settings)
        assert client.vector_store_client is not None
        assert client.registry is not None

    def test_change_source_table_format(
        self, mock_vector, mock_registry, dummy_settings
    ):
        """
        test change source table format function

        Parameters
        ----------
        mock_vector
            mock databricks vector store client
        mock_registry
            mock databricks vector registry client
        dummy_settings
            dummy settings for databricks vector store client

        Returns
        -------
        assert
            return value is as expected
        """
        mock_registry.return_value._convert_source_table_format.return_value = None
        client = GrowlitheClient(settings_config=dummy_settings)
        expected = client.change_source_table_format(table_name="test")

        assert expected is None

    def test_create_vectorsearch_endpoint_index(
        self, mock_vector, mock_registry, dummy_settings
    ):
        """
        test create vs endpointand index function

        Parameters
        ----------
        mock_vector
            mock databricks vector store client
        mock_registry
            mock databricks vector registry client
        dummy_settings
            dummy settings for databricks vector store client

        Returns
        -------
        assert
            return value is as expected
        """

        mock_vector_store_instance = mock_vector.return_value
        mock_vector_store_instance._create_vs_endpoint.return_value = False
        mock_vector_store_instance._create_vs_index_delta_sync.return_value = False

        client = GrowlitheClient(settings_config=dummy_settings)
        client.vector_store_client = mock_vector_store_instance
        expected = client.create_vectorsearch_endpoint_index(
            vs_endpoint_name="test_endpoint",
            vs_index_name="test_index",
            source_table_name="test_table",
            primary_key="id",
            embedding_source_column="text",
            embedding_model_endpoint_name="embedding_model",
        )

        assert expected == 0

    def test_sync_index(self, mock_vector, mock_registry, dummy_settings):
        """
        test sync index function

        Parameters
        ----------
        mock_vector
            mock databricks vector store client
        mock_registry
            mock databricks vector registry client
        dummy_settings
            dummy settings for databricks vector store client

        Returns
        -------
        assert
            return value is as expected
        """
        mock_vector_store_instance = mock_vector.return_value
        mock_vector_store_instance.sync_index.return_value = 0

        client = GrowlitheClient(settings_config=dummy_settings)
        client.vector_store_client = mock_vector_store_instance

        expected = client.sync_index(
            vs_endpoint_name="test_endpoint", vs_index_name="test_index"
        )

        assert expected == 0

    def test_retrieve_similar_context_index(
        self, mock_vector, mock_registry, dummy_settings
    ):
        """
        test retrieve similar context index

        Parameters
        ----------
        mock_vector
            mock databricks vector store client
        mock_registry
            mock databricks vector registry client
        dummy_settings
            dummy settings for databricks vector store client

        Returns
        -------
        assert
            return value is as expected
        """
        mock_vector_registry_instance = mock_registry.return_value
        mock_vector_registry_instance._retrieve_based_on_similarity.return_value = [
            "test",
            "test2",
        ]

        client = GrowlitheClient(settings_config=dummy_settings)
        client.registry = mock_vector_registry_instance

        expected = client.retrieve_similar_context_index(
            endpoint_name="test_endpoint",
            vector_index_name="test_index",
            query_text="test query",
            columns=["col1", "col2"],
            num_results=2,
            score_threshold=0.8,
            query_type="HYBRID",
        )

        assert expected == ["test", "test2"]
