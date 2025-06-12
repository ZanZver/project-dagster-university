import dagster as dg
from dagster_essentials.partitions import monthly_partition

# reference a single asset
trips_by_week = dg.AssetSelection.assets("trips_by_week")

# get all assets but omit one
trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition,
    selection=dg.AssetSelection.all() - trips_by_week
)

# Wil materialize trips_by_week
weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    selection=trips_by_week
)