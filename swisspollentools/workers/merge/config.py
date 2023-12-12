"""
Merge Worker Configuration

This module contains the configuration class for the Merge Worker. The
configuration defines parameters that control the behavior of the worker during
the merge process.

Classes:
- MergeWorkerConfig: Data class representing the configuration for the Merge
Worker.
"""
from pathlib import Path

from typing import Union
from dataclasses import dataclass

from swisspollentools.utils import *

@dataclass
class MergeWorkerConfig:
    """
    Merge Worker Configuration

    Attributes:
    - mew_output_file (Union[Path, str]): The output file path for the merged
    data. Default is "./merged.spt".

    Methods:
    - __post_init__: Post-initialization method to handle additional
    configuration.

    Example:
    merge_config = MergeWorkerConfig(
        mew_output_file="./output/merged_data.spt"
    )
    """
    mew_output_file: Union[Path, str]=Path("./merged.spt")

    def __post_init__(self):
        """
        Post-initialization method to handle additional configuration.

        - Converts the output file path to a Path object if it is provided as a
        string.
        """
        if not isinstance(self.mew_output_file, Path):
            self.mew_output_file = Path(self.mew_output_file)
