from typing import Dict
from typing import List

from loguru import logger
from sm_lakebase.ponyta.core import config
from sm_lakebase.ponyta.lakebase_store._ponyta_lakebase_client import (
    _PonytaLakebaseStore,
)


class PonytaClient:
    """
    The client is used to manage the databricks lakebase postgres, which currently
    includes creation of lakebase autoscaling projects.

    The connection and setup is done via the databricks PAT (linked to SPs/users)

    TODO: lakebase store will need to do some test auth for databricks for init
    """

    def __init__(
        self,
        settings_config: config.Settings,
        pg_project_name: str = None,
        pg_branch_name: str = None,
    ):
        """
        Initialise ponyta client

        Parameters
        ----------
        settings_config: config.Settings
            settings config
        pg_project_name: str = None
            name of pg project
        pg_branch_name: str = None
            name of branch for autoscaling pg
        """

        self.settings_config = settings_config

        self.lakebase_client = None
        if self.settings_config.DATABRICKS_CLUSTER_HOST not in ("", None, "test"):
            self.lakebase_client = _PonytaLakebaseStore(
                settings_config=self.settings_config,
            )

    def create_lakebase_project(
        self,
        pg_project_name: str,
        pg_branch_name: str,
        compute_settings: Dict,
    ) -> int:
        """
        function to create a lakebase autoscaling project

        Parameters
        ----------
        pg_project_name: str
            name of lakebase autoscaling project
        pg_branch_name: str
            name of lakebase branch
        compute_settings: Dict
            compute settings dictionary

        Returns
        -------
        int
            success returns a non exit functon value
        """
        # create lakebase project
        if self.lakebase_client._create_lb_project(pg_project_name=pg_project_name):
            raise ValueError("error in creating lakebase project")

        # update compute setting
        if self.lakebase_client._update_lb_branch_compute_settings(
            pg_project_name=pg_project_name,
            pg_branch_name=pg_branch_name,
            compute_settings=compute_settings,
        ):
            raise ValueError("error in updating compute settings")

        return 0

    def create_branch(
        self,
        pg_project_name: str,
        pg_branch_name: str,
        pg_source_branch: str,
        compute_settings: Dict,
    ) -> int:
        """
        function to create branch for lakebase project

        Parameters
        ----------
        pg_project_name: str
            name of lakebase autoscaling project
        pg_branch_name: str
            name of lakebase branch
        pg_source_branch: str
            name of lakebase branch to copy from
        compute_settings: Dict = None
            compute settings dictionary

        Returns
        -------
        int
            success returns a non exit functon value
        """
        try:
            _ = self.lakebase_client._create_lb_branch(
                pg_project_name=pg_project_name,
                pg_branch_name=pg_branch_name,
                pg_source_branch=pg_source_branch,
            )

            if compute_settings is None:
                return 0

            if self.lakebase_client._update_lb_branch_compute_settings(
                pg_project_name=pg_project_name,
                pg_branch_name=pg_branch_name,
                compute_settings=compute_settings,
            ):
                raise ValueError("error in updating compute settings")
            return 0
        except Exception as e:
            logger.error(e)
            return 1

    def update_branch_compute_settings(
        self, pg_project_name: str, pg_branch_name: str, compute_settings: Dict
    ) -> List:
        """
        wrapper function to update branch compute settings

        Parameters
        ----------
        pg_project_name: str
            name of lakebase autoscaling project
        pg_branch_name: str
            name of lakebase branch
        compute_settings: Dict
            compute settings dictionary

        Returns
        ----------
        List
            success returns a non exit functon value
        """

        if self.lakebase_client._update_lb_branch_compute_settings(
            pg_project_name=pg_project_name,
            pg_branch_name=pg_branch_name,
            compute_settings=compute_settings,
        ):
            raise ValueError("error in updating compute settings")

        return 0
