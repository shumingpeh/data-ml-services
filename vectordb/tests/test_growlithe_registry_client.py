from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sm_vector_store.growlithe.registry._growlithe_registry_client import (
    _GrowlitheRegistry,
)


@patch(
    "sm_vector_store.growlithe.registry._growlithe_registry_client.VectorSearchClient"  # noqa: E501
)
@patch(
    "sm_vector_store.growlithe.registry._growlithe_registry_client.DatabricksSQLClient"
)
class TestGrowlitheRegistry:
    """
    test class for growlithe registry
    """

    def test_init_successful_connection(self, mock_sql, mock_vsc, dummy_settings):
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
            _GrowlitheRegistry, "_test_connection_databricks", return_value=True
        ):
            vector_registry = _GrowlitheRegistry(settings_config=dummy_settings)

            assert isinstance(vector_registry, _GrowlitheRegistry)
            mock_vsc.assert_called_once()
            mock_sql.assert_called_once()

    def test_init_successful_failure(self, mock_sql, mock_vsc, dummy_settings):
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
            _GrowlitheRegistry, "_test_connection_databricks", return_value=False
        ):
            with pytest.raises(
                Exception, match="Databricks creds provided are incorrect"
            ):
                _GrowlitheRegistry(
                    settings_config=dummy_settings,
                )

    def test_connection_databricks(
        self, mock_sql, mock_vsc, dummy_settings, dummy_list_endpoints
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

        vector_registry = _GrowlitheRegistry(settings_config=dummy_settings)
        assert vector_registry._test_connection_databricks() == 1

    def test_retrieve_based_on_similarity(
        self, mock_sql, mock_vsc, dummy_settings, dummy_list_endpoints
    ):
        """
        test retrieval of similarity context

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
        mock_vs_index = MagicMock()
        mock_vsc.return_value.get_index.return_value = mock_vs_index
        mock_vs_index.similarity_search.return_value = {
            "result": {"data_array": ["item1", "item2"]}
        }

        vector_registry = _GrowlitheRegistry(settings_config=dummy_settings)
        expected = vector_registry._retrieve_based_on_similarity(
            endpoint_name="test_endpoint",
            vector_index_name="test_index",
            query_text="test query",
            columns=["col1", "col2"],
            num_results=1,
            score_threshold=0.8,
            query_type="HYBRID",
        )

        assert expected == ["item1", "item2"]
        mock_vs_index.similarity_search.assert_called_once()
        mock_vsc.assert_called_once()

    def test_convert_source_table_format(
        self, mock_sql, mock_vsc, dummy_settings, dummy_list_endpoints
    ):
        """
        test convert source table to format that is required

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
        mock_sql.return_value.query_as_pandas.return_value = None
        vector_registry = _GrowlitheRegistry(settings_config=dummy_settings)

        expected = vector_registry._convert_source_table_format(
            table_name="test_table",
            column_name_set_not_null="col1",
            column_name_primary_key="col2",
        )

        assert expected is None
