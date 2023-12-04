from pathlib import Path

from typing import Union
from dataclasses import dataclass

from swisspollentools.utils import *

@dataclass
class MergeWorkerConfig:
    mew_output_file: Union[Path, str]=Path("./merged.spt")

    def __post_init__(self):
        if not isinstance(self.mew_output_file, Path):
            self.mew_output_file = Path(self.mew_output_file)
