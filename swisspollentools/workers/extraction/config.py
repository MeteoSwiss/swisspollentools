"""
SwissPollenTools Extraction Worker Configuration

The `config.py` module defines the configuration class for the Extraction 
Worker in the SwissPollenTools library. The Extraction Worker is responsible
for handling batched data extraction, providing flexibility through
configuration options.
"""
from pathlib import Path

from typing import Optional, List, Dict, Union
from dataclasses import dataclass, InitVar

from swisspollentools.utils import *

__all__ = ["ExtractionWorkerConfig", "FILTER_PREFIX"]

FILTER_PREFIX = "filter_"

@dataclass
class ExtractionWorkerConfig:
    """
    Configuration class for the Extraction Worker.

    Parameters:
    - `exw_batch_size` (int, optional): The batch size for data extraction.
    - `exw_keep_metadata` (bool, optional): Flag to retain metadata during
    extraction.
    - `exw_keep_fluorescence` (bool, optional): Flag to retain fluorescence
    data during extraction.
    - `exw_keep_rec_properties` (bool, optional): Flag to retain recording
    properties during extraction.
    - `exw_keep_rec` (bool, optional): Flag to retain recording data during
    extraction.
    - `exw_keep_label` (bool, optional): Flag to retain label data during
    extraction.
    - `exw_keep_metadata_key` (List[str], optional): List of specific metadata
    keys to retain during extraction.
    - `exw_keep_fluorescence_keys` (List[str], optional): List of specific
    fluorescence data keys to retain during extraction.
    - `exw_keep_rec_properties_keys` (List[str], optional): List of specific
    recording properties keys to retain during extraction.
    - `exw_tmp_directory` (Union[Path, str], optional): Temporary directory
    path for extraction.
    - `exw_filters` (InitVar[Dict], optional): Initial variable for filters to
    apply during extraction.

    Methods:
    - `__post_init__(self, exw_filters)`: Post-initialization method to handle
    filter constraints.

    Raises:
    - `ValueError`: If an unsupported constraint is provided in the filters
    dictionary.

    """
    exw_batch_size: int=None
    exw_keep_metadata: bool=True
    exw_keep_fluorescence: bool=True
    exw_keep_rec_properties: bool=True
    exw_keep_rec: bool=True
    exw_keep_label: bool=True
    exw_keep_metadata_key: Optional[List[str]]=None
    exw_keep_fluorescence_keys: Optional[List[str]]=None
    exw_keep_rec_properties_keys: Optional[List[str]]=None
    exw_tmp_directory: Optional[Union[Path, str]]=""
    exw_filters: InitVar[Dict]={}

    def __post_init__(self, exw_filters):
        """
        Post-initialization method to handle filter constraints.

        Parameters:
        - `filters` (Optional[Dict]): Dictionary containing filter constraints,
        e.g., {"min_area": 625}.
        Supported constraints include "min" and "max".

        Raises:
        - `ValueError`: If an unsupported constraint is provided in the filters
        dictionary.
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
