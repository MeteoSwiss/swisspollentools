"""
Merge Worker: worker.py

This module defines the worker functions for the Merge Worker, responsible for
handling merge requests and producing merge responses.

Functions:
- Merge: Merges data from multiple requests and generates a merge response.
- MergeWorker: Collate worker function for processing merge requests.
"""
from typing import Any, List, Dict

from swisspollentools.workers.merge.config import *
from swisspollentools.workers.merge.messages import *
from swisspollentools.utils import *
from swisspollentools.utils.constants import _REC_KEY

def Merge(
    requests: List[Dict],
    config: MergeWorkerConfig,
    **kwargs
):
    """
    Merges data from multiple requests and generates a merge response.

    Parameters:
    - requests (List[Dict]): List of merge request dictionaries.
    - config (MergeWorkerConfig): Configuration object for the Merge Worker.
    - **kwargs: Additional keyword arguments.

    Returns:
    MergeResponse: A dictionary representing the merge response.

    Example:
    merge_response = Merge(merge_requests, merge_config)
    """
    data = [get_body(request) for request in requests]
    data = collate_fn(data, list_strategy="concatenate", numpy_strategy="concatenate")
    return MergeResponse(
        file_path=config.mew_output_file,
        **data
    )

@CollateWorker
def MergeWorker(
    requests: List[Dict],
    config: MergeWorkerConfig,
    **kwargs
) -> Dict:
    """
    Collate worker function for processing merge requests.

    Parameters:
    - requests (List[Dict]): List of merge request dictionaries.
    - config (MergeWorkerConfig): Configuration object for the Merge Worker.
    - **kwargs: Additional keyword arguments.

    Returns:
    Dict: A dictionary representing the merge worker response.

    Example:
    merge_worker_response = MergeWorker(merge_requests, merge_config)
    """
    if not all([ismereq(request) for request in requests]):
        raise ValueError()

    return Merge(requests, config, **kwargs)
