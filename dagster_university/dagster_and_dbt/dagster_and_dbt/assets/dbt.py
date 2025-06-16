import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from dagster_and_dbt.project import dbt_project
from dagster import AssetKey


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # Gets the name of the resource type (ex., model, source, seed, snapshot) 
        resource_type = dbt_resource_props["resource_type"]
        # Gets the ACTUAL resource name (trips or stg_trips)
        name = dbt_resource_props["name"]
        
        # We want to rename dbt sources, but we can keep the asset keys of the models the same
        if resource_type == "source":
            return dg.AssetKey(f"taxi_{name}") # To match the key names, we are using taxi_ prefix. E.g. taxi_zone
        else:
            return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_analytics(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
