import dagster as dg
from dagster_essentials.jobs import trip_update_job, weekly_update_job

# Run trip_update_job every 5th of the month
trip_update_schedule = dg.ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="0 0 5 * *"
)

weekly_update_schedule = dg.ScheduleDefinition(
    job=weekly_update_job,
    cron_schedule="0 0 * * 1"
)