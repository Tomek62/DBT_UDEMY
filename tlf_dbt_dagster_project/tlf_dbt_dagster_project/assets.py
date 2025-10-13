import json
from typing import Any, Mapping
from dagster import (AssetExecutionContext, DailyPartitionsDefinition, OpExecutionContext)
from dagster_dbt import (DagsterDbtTranslator, DbtCliResource, dbt_assets, default_metadata_from_dbt_resource_props)

from .constants import dbt_manifest_path

from .constants import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path, exclude="fact_reviews")
def first_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


daily_partitions = DailyPartitionsDefinition(start_date="2022-01-24")

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = {"partition_expr": "date"}
        default_metadata = default_metadata_from_dbt_resource_props(dbt_resource_props)
        return {**default_metadata, **metadata}
    
@dbt_assets(
    manifest=dbt_manifest_path,select="fact_reviews",
    partitions_def=daily_partitions,
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def partitioned_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    (first_partition, last_partition) = context.partition_time_window(list(context.selected_output_names)[0]) # provient de dagster
    context.log.info(f"Running dbt for partition range: {first_partition} to {last_partition}")
    dbt_vars = {"start_date": str(first_partition), "end_date": str(last_partition)}
    dbt_args = ["build", "--vars", json.dumps(dbt_vars)]
    yield from dbt.cli(dbt_args, context=context, fail_on_error=False).stream()



