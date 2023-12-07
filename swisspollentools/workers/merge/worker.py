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
    if not all([ismereq(request) for request in requests]):
        raise ValueError()
    
    return Merge(requests, config, **kwargs)
