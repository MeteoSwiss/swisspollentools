from typing import Optional, List, Dict
from dataclasses import dataclass, InitVar

from swisspollentools.utils import *

__all__ = ["ExtractionWorkerConfig", "FILTER_PREFIX"]

FILTER_PREFIX = "filter_"

@dataclass
class ExtractionWorkerConfig:
    """
    Configuration class for the ExtractionWorker, extending 
    BatchedOutputConfig.

    Attributes:
    - exw_keep_metadata_key (List[str], optional): List of metadata keys to retain
    during extraction.
    - exw_keep_fluorescence_keys (List[str], optional): List of fluorescence data
    keys to retain during extraction.
    - exw_keep_rec_properties_keys (List[str], optional): List of recording
    properties keys to retain during extraction.
    - exw_filters (InitVar[Optional[Dict]], default=None): Initial variable for
    filters to apply during extraction.
    """
    exw_batch_size: int=None
    exw_keep_metadata: bool=True
    exw_keep_fluorescence: bool=True
    exw_keep_rec_properties: bool=True
    exw_keep_rec: bool=True
    exw_keep_metadata_key: Optional[List[str]]=None
    exw_keep_fluorescence_keys: Optional[List[str]]=None
    exw_keep_rec_properties_keys: Optional[List[str]]=None
    exw_filters: InitVar[Dict]={}

    def __post_init__(self, exw_filters):
        """
        Post-initialization method to handle filter constraints.

        Parameters:
        - filters (Optional[Dict]): Dictionary containing filter constraints,
        e.g., {"min_area": 625}.
          Supported constraints include "min" and "max".

        Raises:
        - ValueError: If an unsupported constraint is provided in the filters dictionary.
        """
        order_constraints = {
            "min": lambda v: lambda x: v < x,
            "max": lambda v: lambda x: v > x
        }

        for k, v in exw_filters.items():
            constraint, k = k.split("_")

            if not constraint in order_constraints.keys():
                raise ValueError()

            k = ATTRIBUTE_SEP.join([EXTRACTION_WORKER_PREFIX, FILTER_PREFIX, k])
            v = order_constraints[constraint](v)

            setattr(self, k, v)
