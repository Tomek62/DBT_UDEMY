"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import first_project_dbt_assets

schedules = [
    # build_schedule_from_dbt_selection(
    #     [first_project_dbt_assets],
    #     job_name="materialize_dbt_models",
    #     cron_schedule="0 0 * * *",
    #     dbt_select="fqn:*",
    # ),
    build_schedule_from_dbt_selection(
        [first_project_dbt_assets],
        job_name="weekly_tables_refresh",
        cron_schedule="0 2 * * 0",
        dbt_select="config.materialized:table",
    ),
    build_schedule_from_dbt_selection(
        [first_project_dbt_assets],
        job_name="daily_views_refresh",
        cron_schedule="0 0 * * *",
        dbt_select="config.materialized:view",
    ),
]