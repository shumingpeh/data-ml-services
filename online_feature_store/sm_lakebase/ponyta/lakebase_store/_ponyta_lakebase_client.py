from typing import Dict
from typing import List
from typing import Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Branch
from databricks.sdk.service.postgres import BranchSpec
from databricks.sdk.service.postgres import Duration
from databricks.sdk.service.postgres import Endpoint
from databricks.sdk.service.postgres import EndpointSpec
from databricks.sdk.service.postgres import EndpointType
from databricks.sdk.service.postgres import FieldMask
from databricks.sdk.service.postgres import Project
from databricks.sdk.service.postgres import ProjectSpec
from loguru import logger
from sm_lakebase.ponyta.core import config


class _PonytaLakebaseStore:
    """
    The lakebase client is used to manage online feature store and pg
    """

    def __init__(
        self,
        settings_config: config.Settings,
        pg_project_name: str = None,
        pg_branch_name: str = None,
    ):
        """
        Initialise ponyta lakebase client

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
        self.pg_project_name = pg_project_name
        self.pg_branch_name = pg_branch_name
        self.lbc = WorkspaceClient(
            host=self.settings_config.DATABRICKS_CLUSTER_HOST,
            token=self.settings_config.DATABRICKS_TOKEN,
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
            list(self.lbc.postgres.list_projects())
            return True
        except Exception as e:
            logger.exception(e)
            return False

    def _check_lakebase_project_exists(self, pg_project_name: str) -> int:
        """
        function to check if name of lakebase project exists

        Parameters
        ----------
        pg_project_name: str
            name of lakebase autoscaling project

        Returns
        ----------
        int
            success returns a non exit functon value
        """
        try:
            if "projects/" + pg_project_name in [
                project.name for project in self.lbc.postgres.list_projects()
            ]:
                return 0
            return 1
        except Exception as e:
            logger.error(e)
            return 1

    def _create_lb_project(self, pg_project_name: str):
        """
        function to create lakebase project

        Parameters
        ----------
        pg_project_name: str
            name of lakebase autoscaling project

        Returns
        ----------
        int
            success returns a non exit functon value

        """
        if self._check_lakebase_project_exists(pg_project_name=pg_project_name):
            logger.info(f"creating lakebase project: {pg_project_name}")

            operation = self.lbc.postgres.create_project(
                project_id=pg_project_name,
                project=Project(
                    spec=ProjectSpec(display_name=pg_project_name, pg_version=17)
                ),
            )

            result = operation.wait()

            if result.name == "projects/" + pg_project_name:
                logger.info(f"finish lakebase project: {pg_project_name}")
                return 0

        logger.info(f"lakebase project: {pg_project_name} alr exists")
        return 0

    def _check_lakebase_branch_exists(
        self, pg_project_name: str, pg_branch_name: str
    ) -> int:
        """
        function to check if name of lakebase branch exists

        Parameters
        ----------
        pg_project_name: str
            name of lakebase autoscaling project
        pg_branch_name: str
            name of lakebase branch

        Returns
        ----------
        int
            success returns a non exit functon value
        """
        try:
            if pg_branch_name in [
                branch.name.split("/")[-1]
                for branch in self.lbc.postgres.list_branches(
                    parent="projects/" + pg_project_name
                )
            ]:
                return 0
            return 1
        except Exception as e:
            logger.error(e)
            return 1

    def _create_lb_branch(
        self,
        pg_project_name: str,
        pg_branch_name: str,
        pg_source_branch: str = "production",
    ) -> int:
        """
        function to create branch in lakebase project

        Parameters
        ----------
        pg_project_name: str
            name of lakebase autoscaling project
        pg_branch_name: str
            name of lakebase branch
        pg_source_branch: str = "production"
            name of source branch to copy from

        Returns
        ----------
        int
            success returns a non exit functon value

        """
        if self._check_lakebase_branch_exists(
            pg_project_name=pg_project_name, pg_branch_name=pg_branch_name
        ):
            logger.info(f"creating lakebase branch: {pg_branch_name}")

            operation = self.lbc.postgres.create_branch(
                parent=f"projects/{pg_project_name}",
                branch=Branch(
                    spec=BranchSpec(
                        source_branch=f"projects/{pg_project_name}/branches/{pg_source_branch}",  # noqa: E501
                        no_expiry=True,
                    )
                ),
                branch_id=pg_branch_name,
            )

            result = operation.wait()

            if result.name.split("/")[-1] == pg_branch_name:
                logger.info(f"finish lakebase branch: {pg_branch_name}")
                return 0

        logger.info(f"lakebase branch: {pg_branch_name} alr exists")
        return 0

    def partial_function_setting(self, compute_settings: Dict) -> Tuple[List, Dict]:
        """
        function to simplify settings

        Parameters
        ----------
        compute_settings: Dict
            compute settings dictionary

        Returns
        ----------
        List
            list of fieldmask to update settings
        Dict
            dictionary that contains the partial function to update
        """

        spec_kwargs = {"endpoint_type": EndpointType.ENDPOINT_TYPE_READ_WRITE}

        field_mask_list = []

        if "min_cu" in compute_settings:
            spec_kwargs["autoscaling_limit_min_cu"] = compute_settings["min_cu"]
            field_mask_list.append("spec.autoscaling_limit_min_cu")

        if "max_cu" in compute_settings:
            spec_kwargs["autoscaling_limit_max_cu"] = compute_settings["max_cu"]
            field_mask_list.append("spec.autoscaling_limit_max_cu")

        if "no_suspension" in compute_settings:
            spec_kwargs["no_suspension"] = compute_settings["no_suspension"]
            field_mask_list.append("spec.suspension")

            return field_mask_list, spec_kwargs

        if "timeout_seconds" in compute_settings:
            spec_kwargs["suspend_timeout_duration"] = Duration(
                seconds=compute_settings["timeout_seconds"]
            )
            field_mask_list.append("spec.suspension")

        return field_mask_list, spec_kwargs

    def _update_lb_branch_compute_settings(
        self, pg_project_name: str, pg_branch_name: str, compute_settings: Dict
    ) -> int:
        """
        function to create branch in lakebase project

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
        int
            success returns a non exit function value

        """

        project_name = (
            f"projects/{pg_project_name}/branches/{pg_branch_name}/endpoints/primary"
        )
        field_mask_list, spec_kwargs = self.partial_function_setting(
            compute_settings=compute_settings
        )

        endpoint = Endpoint(
            name=project_name,
            spec=EndpointSpec(**spec_kwargs),
        )

        try:
            status_result = self.lbc.postgres.update_endpoint(
                name=project_name,
                endpoint=endpoint,
                update_mask=FieldMask(field_mask=field_mask_list),
            ).wait()

            if status_result.update_time.seconds:
                return 0
            return 1
        except Exception as e:
            logger.error(e)
            return 1
