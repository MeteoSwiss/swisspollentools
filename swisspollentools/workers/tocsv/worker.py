import pandas as pd

from pathlib import Path
from typing import Any, Dict, Generator

from swisspollentools.utils import *
from swisspollentools.workers.tocsv.config import *
from swisspollentools.workers.tocsv.messages import *

def ToCSV(
    request: Dict,
    config: ToCSVWorkerConfig,
    **kwargs
):
    if request[BATCH_ID_KEY] is not None:
        file_path = ".".join([
            Path(request[FILE_PATH_KEY]).stem, 
            str(request[BATCH_ID_KEY]), 
            "csv"
        ])
    else:
        file_path = ".".join([Path(request[FILE_PATH_KEY]).stem, "csv"])
    file_path = config.tocsvw_output_directory.joinpath(file_path)
    file_path = str(file_path)

    data = get_body(request)
    data = {
        k: v.tolist() if isinstance(v, np.ndarray) else v \
            for k, v in data.items()
    }
    data = pd.DataFrame(data)
    data.to_csv(file_path, index=False)

    yield ToCSVResponse(
        file_path=file_path
    )

@PullPushWorker
def ToCSVWorker(
    request: Dict,
    config: ToCSVWorkerConfig,
    **kwargs
) -> Generator:
    if not istocsvreq(request):
        raise ValueError()
    
    yield from ToCSV(request, config, **kwargs)
