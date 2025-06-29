import json
from typing import Any, Mapping

from dagster import (AssetExecutionContext, DailyPartitionsDefinition, OpExecutionContext)
from dagster_dbt import (DagsterDbtTranslator, DbtCliResource, dbt_assets, default_metadata_from_dbt_resource_props)

from .constants import dbt_manifest_path

# Simple DBT integration. We exclude "fct_reviews" since it will be declared as an incremental asset
@dbt_assets(manifest=dbt_manifest_path, exclude="fct_reviews")
def dbtlearn_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()




# From now on we will use a partition-based approach to implement the incremental dbt approach
# and avoid re-processing all data

# Firstly, we define the type of partitions to be used with its initial point in time
daily_partitions = DailyPartitionsDefinition(start_date="2025-01-01")

# DBT Translator serves as a data injection package to pass variables to dbt
class CustomDragsterDbtTranslator(DagsterDbtTranslator):
    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        # Table is partitioned based on a column called date
        metadata = {"partition_expr": "date"}
        default_metadata = default_metadata_from_dbt_resource_props(dbt_resource_props)
        return {**default_metadata, **metadata}
    
@dbt_assets(manifest=dbt_manifest_path,
            select="fct_reviews",
            partitions_def=daily_partitions,
            dagster_dbt_translator=CustomDragsterDbtTranslator())
def dbtlearn_partitioned_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # The context provided by Dagster is partition aware and we can simply retrieve the time limits from it
    (
        first_partition,
        last_partition,
    ) = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )

    dbt_vars = {"start_date": str(first_partition), "end_date": str(last_partition)}
    dbt_args = ["build", "--vars", json.dumps(dbt_vars)]
    dbt_cli_task = dbt.cli(dbt_args, context=context, raise_on_error=False)

    yield from dbt_cli_task.stream()