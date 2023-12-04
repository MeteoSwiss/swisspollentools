import h5py

from pathlib import Path
from typing import Any, Dict, Generator

from swisspollentools.utils import PlPsCWorker
from swisspollentools.workers.tohdf5.config import *
from swisspollentools.workers.tohdf5.messages import *

def ToHDF5(
    request: Dict,
    config: ToHDF5WorkerConfig,
    **kwargs
):
    if request[BATCH_ID_KEY] is not None:
        file_path = ".".join([
            Path(request[FILE_PATH_KEY]).stem, 
            str(request[BATCH_ID_KEY]), 
            "hdf5"
        ])
    else:
        file_path = ".".join([Path(request[FILE_PATH_KEY]).stem, "csv"])
    file_path = config.tohdf5w_output_directory.joinpath(file_path)
    file_path = str(file_path)

    data = get_body(request)

    with h5py.File(file_path, "w") as f:
        for k, v in data.items():
            dtype = type(v[0])

            if dtype == str:
                v = [el.encode("utf-8") for el in v]
                dtype = h5py.special_dtype(vlen=bytes)

            f.create_dataset(k, data=v, dtype=dtype)

    yield ToHDF5Response(
        file_path=file_path
    )

@PlPsCWorker
def ToHDF5Worker(
    request: Dict,
    config: ToHDF5WorkerConfig,
    **kwargs
) -> Generator:
    if not istohdf5req(request):
        raise ValueError()

    yield from ToHDF5(request, config, **kwargs)
