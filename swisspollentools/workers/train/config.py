from pathlib import Path

from typing import Optional, List, Dict, Union, Tuple
from dataclasses import dataclass, InitVar

@dataclass
class TrainWorkerConfig():
    trw_from_rec0: bool=True
    trw_from_rec1: bool=True
    trw_from_fluorescence: bool=True
    trw_from_fluorescence_keys: InitVar[Optional[Union[Dict, List]]]=None
    trw_rec_shape: Tuple[int, int]=(200, 200)
    trw_rec_precision: int=16
    trw_batch_size: int=1024
    trw_n_epochs: int=1
    trw_output_directory: Union[Path, str]="./"

    def __post_init__(
        self,
        trw_from_fluorescence_keys: Optional[Union[Dict, List]]=None
    ):
        if not trw_from_fluorescence_keys:
            self.trw_from_fluorescence_keys = {}
        elif isinstance(trw_from_fluorescence_keys, list):
            self.trw_from_fluorescence_keys = \
                {el: el for el in trw_from_fluorescence_keys}

        if (self.trw_from_fluorescence) and (not self.trw_from_fluorescence_keys):
            self.trw_from_fluorescence = False
        
        if not isinstance(self.trw_output_directory, Path):
            self.trw_output_directory = Path(self.trw_output_directory)

        if not self.trw_output_directory.is_dir():
            Path.mkdir(self.trw_output_directory)
