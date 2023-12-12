"""
ToCSV Worker: worker.py

This module defines the ToCSV Worker functionality, including the main worker
function and the worker function decorated with the PullPushWorker decorator.

Worker Functions:
- ToCSV: Main ToCSV Worker function for processing requests and generating CSV
files.
- ToCSVWorker: ToCSV Worker function decorated with the PullPushWorker
decorator.
"""
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
    """
    ToCSV Worker Main Function

    Processes a ToCSV Worker request, generates a CSV file from the data, and
    yields a ToCSV Worker response.

    Parameters:
    - request (Dict): The ToCSV Worker request dictionary.
    - config (ToCSVWorkerConfig): The ToCSV Worker configuration.
    - **kwargs: Additional keyword arguments.

    Yields:
    Generator: ToCSV Worker response.

    Raises:
    - ValueError: If the request is not a valid ToCSV Worker request.
    """
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
    """
    ToCSV Worker Function Decorated with PullPushWorker

    This function is decorated with the PullPushWorker decorator, allowing it
    to be used as a worker in a pull-push architecture.

    Parameters:
    - request (Dict): The ToCSV Worker request dictionary.
    - config (ToCSVWorkerConfig): The ToCSV Worker configuration.
    - **kwargs: Additional keyword arguments.

    Yields:
    Generator: ToCSV Worker response.

    Raises:
    - ValueError: If the request is not a valid ToCSV Worker request.

    Example:
    to_csv_request = ToCSVRequest(
        file_path="./data/output.csv", batch_id=1, response=my_response
    )
    to_csv_worker = ToCSVWorker(to_csv_request, my_config)
    """
    if not istocsvreq(request):
        raise ValueError()
    
    yield from ToCSV(request, config, **kwargs)
