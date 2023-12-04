from pathlib import Path

from typing import Union
from dataclasses import dataclass

__all__ = ["ToCSVWorkerConfig"]

@dataclass
class ToCSVWorkerConfig:
    tocsvw_output_directory: Union[Path, str]=None

    def __post_init__(self):
        if not isinstance(self.tocsvw_output_directory, Path):
            self.tocsvw_output_directory = Path(self.tocsvw_output_directory)

        if not self.tocsvw_output_directory.is_dir():
            Path.mkdir(self.tocsvw_output_directory)
