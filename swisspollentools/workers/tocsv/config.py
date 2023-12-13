"""
ToCSV Worker Configuration: config.py

This module defines the configuration class for the ToCSV Worker, which
specifies settings for the ToCSV Worker's behavior.

Configuration Class:
- ToCSVWorkerConfig: Configuration class for the ToCSV Worker.
"""
from pathlib import Path

from typing import Union
from dataclasses import dataclass

@dataclass
class ToCSVWorkerConfig:
    """
    ToCSV Worker Configuration Class

    Attributes:
    - tocsvw_output_directory (Union[Path, str]): The output directory path for
    saving CSV files.

    Example:
    to_csv_config = ToCSVWorkerConfig(
        tocsvw_output_directory="./output/csv_data"
    )
    """
    tocsvw_output_directory: Union[Path, str]=None

    def __post_init__(self):
        """
        Post-initialization method to handle additional configuration.

        - Converts the output directory path to a Path object if it is provided
        as a string.
        - Creates the output directory if it does not exist.

        Example:
        to_csv_config = ToCSVWorkerConfig(
            tocsvw_output_directory="./output/csv_data"
        )
        """
        if not isinstance(self.tocsvw_output_directory, Path):
            self.tocsvw_output_directory = Path(self.tocsvw_output_directory)

        if not self.tocsvw_output_directory.is_dir():
            Path.mkdir(self.tocsvw_output_directory, parents=True)
