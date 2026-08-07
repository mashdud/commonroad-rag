"""
Central configuration for the CommonRoad ingestion pipeline.

All environment-specific values (storage account, catalog, schema) are
injected via Databricks Job parameters / bundle target variables, NOT
hardcoded here. This lets the exact same code run in dev, staging, and
prod with different targets — the core of "production-like" pipelines.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    storage_account: str
    container: str
    catalog: str
    bronze_schema: str
    silver_schema: str

    @property
    def base_path(self) -> str:
        return f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net"

    @property
    def raw_path(self) -> str:
        return f"{self.base_path}/raw"

    @property
    def checkpoint_path(self) -> str:
        return f"{self.base_path}/_checkpoints/bronze_scenario_raw"

    @property
    def schema_location(self) -> str:
        return f"{self.base_path}/_schemas/bronze_scenario_raw"

    @property
    def bronze_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.scenario_raw"

    @property
    def silver_scenarios_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.scenarios"

    @property
    def silver_lanelets_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.lanelets"

    @property
    def silver_obstacles_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.obstacles"

    @property
    def silver_obstacle_states_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.obstacle_states"

    @property
    def silver_planning_problems_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.planning_problems"

    @property
    def silver_failed_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.parse_failures"


def load_config() -> PipelineConfig:
    """
    Load config from environment variables / Databricks widgets.
    Defaults match the dev target; override per-environment via the
    Databricks Job's base_parameters (set in resources/commonroad_job.yml).
    """
    return PipelineConfig(
        storage_account=os.environ.get("STORAGE_ACCOUNT", "commonroadlake12345"),
        container=os.environ.get("CONTAINER", "unitycatalog"),
        catalog=os.environ.get("CATALOG", "commonroad"),
        bronze_schema=os.environ.get("BRONZE_SCHEMA", "bronze"),
        silver_schema=os.environ.get("SILVER_SCHEMA", "silver"),
    )
