from dagster import Definitions, EnvVar, FilesystemIOManager, load_assets_from_modules

from . import assets
from .resources import (
    ConfigurableGirderIOManager,
    GirderConnection,
    GirderCredentials,
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[assets.observation_job],
    schedules=[assets.observation_schedule],
    resources={
        "fs_io_manager": FilesystemIOManager(),
        "girder_io_manager": ConfigurableGirderIOManager(
            api_key=EnvVar("GIRDER_API_KEY"),
            api_url=EnvVar("GIRDER_API_URL"),
            source_folder_id=EnvVar("DATAFLOW_SRC_FOLDER_ID"),
            target_folder_id=EnvVar("DATAFLOW_DST_FOLDER_ID"),
        ),
        "girder": GirderConnection(
            credentials=GirderCredentials(
                api_key=EnvVar("GIRDER_API_KEY"),
                api_url=EnvVar("GIRDER_API_URL"),
            )
        ),
    },
)
