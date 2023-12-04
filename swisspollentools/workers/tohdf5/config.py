from pathlib import Path

from typing import Union
from dataclasses import dataclass

@dataclass
class ToHDF5WorkerConfig:
    tohdf5w_output_directory: Union[Path, str]=None

    def __post_init__(self):
        if not isinstance(self.tohdf5w_output_directory, Path):
            self.tohdf5w_output_directory = Path(self.tohdf5w_output_directory)

        if not self.tohdf5w_output_directory.is_dir():
            Path.mkdir(self.tohdf5w_output_directory)