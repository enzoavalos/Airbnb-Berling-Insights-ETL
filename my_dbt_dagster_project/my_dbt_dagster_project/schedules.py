"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import dbtlearn_dbt_assets

schedules = [
     build_schedule_from_dbt_selection(
         dbt_assets=[dbtlearn_dbt_assets],
         job_name="materialize_dbt_models",
         cron_schedule="0 5 * * *",
         dbt_select="fqn:*",
     ),
]