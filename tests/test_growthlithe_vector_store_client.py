from unittest.mock import patch

import pytest
from sm_vector_store.growlithe.vector_store._growlithe_vector_store_client import (
    _GrowlitheVectorStore,
)


@patch(
    "sm_vector_store.growlithe.vector_store._growlithe_vector_store_client.VectorSearchClient"  # noqa: E501
)
class TestGrowlitheVectorStore:
    """
    test class for growlithe vector store
    """

    def test_init_successful_connection(self, mock_vsc, dummy_settings):
        """
        test init function with databricks

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client

        Returns
        -------
        assert
            return value is as expected
        """

        with patch.object(
            _GrowlitheVectorStore, "_test_connection_databricks", return_value=True
        ):
            vector_store = _GrowlitheVectorStore(
                settings_config=dummy_settings, vs_endpoint_name="test_endpoint"
            )

            assert isinstance(vector_store, _GrowlitheVectorStore)
            mock_vsc.assert_called_once()

    def test_init_successful_failure(self, mock_vsc, dummy_settings):
        """
        test init function with databricks

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client

        Returns
        -------
        assert
            return value is as expected
        """

        with patch.object(
            _GrowlitheVectorStore, "_test_connection_databricks", return_value=False
        ):
            with pytest.raises(
                ValueError, match="Databricks creds provided are incorrect"
            ):
                _GrowlitheVectorStore(
                    settings_config=dummy_settings, vs_endpoint_name="test_endpoint"
                )

    def test_connection_databricks(
        self, mock_vsc, dummy_settings, dummy_list_endpoints
    ):
        """
        test connection databricks function

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_list_endpoints
            dummy return value when listing vs endpoint

        Returns
        -------
        assert
            return value is as expected
        """
        mock_vsc.return_value.list_endpoints.return_value = dummy_list_endpoints

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        assert vector_store._test_connection_databricks() == 1

    def test_check_vs_endpoint_exists(
        self, mock_vsc, dummy_settings, dummy_list_endpoints
    ):
        """
        test check vs endpoint exists function

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_list_endpoints
            dummy return value when listing vs endpoint

        Returns
        -------
        assert
            return value is as expected
        """
        mock_vsc.return_value.list_endpoints.return_value = dummy_list_endpoints

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        assert (
            vector_store._check_vs_endpoint_exists(vs_endpoint_name="test-example") == 1
        )
        assert vector_store._check_vs_endpoint_exists(vs_endpoint_name="testing") == 0

    def test_get_endpoint_state_status_endpoint(
        self, mock_vsc, dummy_settings, dummy_get_endpoint
    ):
        """
        test get endpoint state status function

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_get_endpoint
            dummy return value when returning vs endpoint state

        Returns
        -------
        assert
            return value is as expected
        """

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        expected = vector_store._get_endpoint_state_status(
            endpoint=dummy_get_endpoint, type_of_creation="endpoint"
        )
        assert expected == "ONLINE"

    def test_get_endpoint_state_status_index(
        self, mock_vsc, dummy_settings, dummy_get_index
    ):
        """
        test get endpoint state status function

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_get_index
            dummy return value when returning vs index state

        Returns
        -------
        assert
            return value is as expected
        """

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        expected = vector_store._get_endpoint_state_status(
            endpoint=dummy_get_index, type_of_creation="index"
        )
        assert "ONLINE" in expected

    def test_create_vs_endpoint_exist(
        self, mock_vsc, dummy_settings, dummy_list_endpoints
    ):
        """
        test create vs endpoint status alr exist function

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_list_endpoints
            dummy return value when returning list of endpoints

        Returns
        -------
        assert
            return value is as expected
        """

        mock_vsc.return_value.list_endpoints.return_value = dummy_list_endpoints

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        result = vector_store._create_vs_endpoint(vs_endpoint_name="testing")

        assert result == 0

    @patch(
        "sm_vector_store.growlithe.vector_store._growlithe_vector_store_client.polling"
    )
    def test_create_vs_endpoint_not_exist(
        self, mock_polling, mock_vsc, dummy_settings, dummy_list_endpoints_not_exist
    ):
        """
        test create vs endpoint status not exist function

        Parameters
        ----------
        mock_polling
            mock polling module
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_list_endpoints_not_exist
            dummy return value when returning list of endpoints that does not exist

        Returns
        -------
        assert
            return value is as expected
        """

        mock_vsc.return_value.list_endpoints.return_value = (
            dummy_list_endpoints_not_exist
        )
        mock_polling.return_value = True

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        result = vector_store._create_vs_endpoint(vs_endpoint_name="testing")

        assert result == 0

    def test_check_vs_index_exists(self, mock_vsc, dummy_settings, dummy_list_indexes):
        """
        test check vs endpoint exists function

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_list_indexes
            dummy return value when returning list of endpoints that does not exist

        Returns
        -------
        assert
            return value is as expected
        """
        mock_vsc.return_value.list_indexes.return_value = dummy_list_indexes

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        assert (
            vector_store._check_vs_index_exists(
                vs_endpoint_name="test-example", vs_index_name="testing.index"
            )
            == 1
        )
        assert (
            vector_store._check_vs_index_exists(
                vs_endpoint_name="testing", vs_index_name="test.index"
            )
            == 0
        )

    def test_create_vs_index_delta_sync_exist(
        self, mock_vsc, dummy_settings, dummy_list_indexes
    ):
        """
        test get endpoint state status function

        Parameters
        ----------
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_list_indexes
            dummy return value when returning list of indexes

        Returns
        -------
        assert
            return value is as expected
        """

        mock_vsc.return_value.list_indexes.return_value = dummy_list_indexes

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        expected = vector_store._create_vs_index_delta_sync(
            vs_endpoint_name="testing",
            vs_index_name="test.index",
            source_table_name="test_table",
            primary_key="id",
            embedding_source_column="content",
            embedding_model_endpoint_name="test-endpoint",
        )

        assert expected == 0

    @patch(
        "sm_vector_store.growlithe.vector_store._growlithe_vector_store_client.polling"
    )
    def test_create_vs_index_delta_sync_not_exist(
        self, mock_polling, mock_vsc, dummy_settings, dummy_list_indexes
    ):
        """
        test get endpoint state status function

        Parameters
        ----------
        mock_polling
            mock polling module
        mock_vsc
            mock databricks vector store client
        dummy_settings
            dummy settings for databricks vector store client
        dummy_list_indexes
            dummy return value when returning list of indexes

        Returns
        -------
        assert
            return value is as expected
        """

        mock_vsc.return_value.list_indexes.return_value = dummy_list_indexes
        mock_polling.return_value = True

        vector_store = _GrowlitheVectorStore(settings_config=dummy_settings)
        expected = vector_store._create_vs_index_delta_sync(
            vs_endpoint_name="testing",
            vs_index_name="test1_index",
            source_table_name="test_table",
            primary_key="id",
            embedding_source_column="content",
            embedding_model_endpoint_name="test-endpoint",
        )

        assert expected == 0
