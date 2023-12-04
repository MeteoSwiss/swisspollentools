import re

from io import BytesIO

import tempfile
import zipfile
import json

import h5py
import pandas as pd

from typing import Any, Dict, Generator, List, Tuple

from PIL import Image
import numpy as np

from swisspollentools.workers.extraction.config import *
from swisspollentools.workers.extraction.messages import *
from swisspollentools.utils import *
from swisspollentools.utils.constants import _METADATA_KEY, _FLUODATA_KEY, _REC_PROPERTIES_KEY, _REC_KEY

def __zip_get_index(
    record: zipfile.Path, 
    *args: List[str]
) -> List[str]:
    """
    Generates an index of file names within a given zipfile. Path based on
    specified patterns.

    Parameters:
    - record (zipfile.Path): The zipfile.Path object representing the zip 
    archive.
    - *args (List[str]): Variable number of string arguments representing 
    patterns to match file names.

    Returns:
    List[str]: A list of unique file names within the zip archive that match 
    all specified patterns.

    Note:
    - File names are obtained by removing the specified patterns from the end
    of the file names.
    """
    index_fn = lambda pattern: set([
        el.name.removesuffix(pattern) \
            for el in record.iterdir() \
            if el.name.endswith(pattern)
    ])

    index = set.intersection(*[index_fn(arg) for arg in args if isinstance(arg, str)])
    return list(index)

def __zip_read_event(
    record: zipfile.Path,
    id: str,
    keep_metadata_key: List[str]=[],
    keep_fluorescence_keys: List[str]=[],
    keep_rec_properties_keys: List[str]=[]
) -> Tuple[dict, dict, Tuple[dict, dict]]:
    """
    Reads and extracts relevant information from an event within a zip archive.

    Parameters:
    - record (zipfile.Path): The zipfile.Path object representing the zip 
    archive.
    - id (str): The identifier of the event to be read.
    - keep_metadata_key (List[str], optional): List of metadata keys to retain,
    default is an empty list.
    - keep_fluorescence_keys (List[str], optional): List of fluorescence data
    keys to retain, default is an empty list.
    - keep_rec_properties_keys (List[str], optional): List of recording
    properties keys to retain, default is an empty list.

    Returns:
    Tuple[dict, dict, Tuple[dict, dict]]: A tuple containing:
    - A dictionary representing the metadata of the event.
    - A dictionary representing the fluorescence data of the event.
    - A tuple containing two dictionaries representing recording properties for
    two images (rec0 and rec1).
    """
    event = record.joinpath(id + POLLENO_EVENT_SUFFIX).read_bytes()
    event = json.loads(event)

    metadata = event["metadata"]
    fluorescence_data = event["computedData"]["fluorescenceSpectra"]
    rec0_properties = event["computedData"]["img0Properties"]
    rec1_properties = event["computedData"]["img1Properties"]

    if keep_metadata_key:
        metadata = {k: metadata[k] for k in keep_metadata_key}
    if keep_fluorescence_keys:
        fluorescence_data = {k: fluorescence_data[k] \
                                for k in keep_fluorescence_keys}
    if keep_rec_properties_keys:
        rec0_properties = {k: rec0_properties[k] \
                                for k in keep_rec_properties_keys}
        rec1_properties = {k: rec1_properties[k] \
                                for k in keep_rec_properties_keys}

    return (
        metadata,
        fluorescence_data,
        (rec0_properties, rec1_properties)
    )

def __zip_read_rec(
    record: zipfile.Path,
    id: str,
    suffix: str
) -> np.ndarray:
    """
    Reads and flattens an image file within a zip archive.

    Parameters:
    - record (zipfile.Path): The zipfile.Path object representing the zip 
    archive.
    - id (str): The identifier of the recording to be read.
    - suffix (str): The suffix indicating the type of recording file (e.g., 
    POLLENO_REC0_SUFFIX, POLLENO_REC1_SUFFIX).

    Returns:
    np.ndarray: A flattened NumPy array representing the pixel values of the image.
    """
    rec = BytesIO(record.joinpath(id + suffix).read_bytes())
    rec = np.array(Image.open(rec)).flatten()

    return rec

def __zip_filter_indexed_events(
    record: zipfile.Path,
    index: List[str],
    config: ExtractionWorkerConfig
) -> Generator:
    """
    Filters and yields events from a zip archive based on a given index and
    configuration.

    Parameters:
    - record (zipfile.Path): The zipfile.Path object representing the zip
    archive.
    - index (List[str]): List of event IDs to filter and process.
    - config (ExtractionWorkerConfig): Configuration object specifying
    filtering and retention criteria.

    Yields:
    Tuple[str, Tuple[dict, dict, Tuple[dict, dict]]]: A tuple containing:
    - The event ID.
    - A tuple representing the filtered metadata, fluorescence data, and
    recording properties for two images (rec0 and rec1).

    Note:
    - It applies filters specified in the config object to exclude events based
    on recording properties.
    """
    for id in index:
        event = __zip_read_event(
            record=record,
            id=id,
            keep_metadata_key=config.exw_keep_metadata_key,
            keep_fluorescence_keys=config.exw_keep_fluorescence_keys,
            keep_rec_properties_keys=config.exw_keep_rec_properties_keys
        )
        metadata, fluodata, rec_properties = event

        filters = [(el.removeprefix(FILTER_PREFIX), getattr(config, el)) \
                        for el in dir(config) if el.startswith(FILTER_PREFIX)]
        filters = [filter(prop[k]) \
                    for prop in rec_properties \
                    for k, filter in filters]
        if any(filters):
            continue

        yield id, (metadata, fluodata, rec_properties)

def __zip_filtered_recs_generator(
    record: zipfile.Path,
    index: Generator
) -> Generator:
    """
    Generates and yields filtered recordings from a zip archive based on a
    given index.

    Parameters:
    - record (zipfile.Path): The zipfile.Path object representing the zip
    archive.
    - index (typing.Generator): Generator yielding tuples with event ID and
    associated data.

    Yields:
    Tuple[str, dict, dict, Tuple[dict, dict], np.ndarray, np.ndarray]: A tuple
    containing:
    - The event ID.
    - Metadata dictionary.
    - Fluorescence data dictionary.
    - Tuple of recording properties for two images (rec0 and rec1).
    - NumPy array representing flattened recording data for rec0.
    - NumPy array representing flattened recording data for rec1.

    Note:
    - Expects the index to yield tuples with event ID and associated data,
    including metadata, fluorescence, and rec_properties.
    - Yields tuples containing event ID, metadata, fluorescence data, recording 
    properties, and flattened recording data.
    """
    for id, data in index:
        rec0 = __zip_read_rec(record, id, POLLENO_REC0_SUFFIX)
        rec1 = __zip_read_rec(record, id, POLLENO_REC1_SUFFIX)

        yield id, (*data, rec0, rec1)

def ZipExtraction(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs: Any
) -> Generator:
    """
    Performs extraction of events and associated recordings from a zip archive.

    Parameters:
    - request (Dict): Extraction Request message containing the file path to
    the zip archive.
    - config (ExtractionWorkerConfig): Configuration object specifying
    filtering and retention criteria.
    - **kwargs: Additional keyword arguments.

    Yields:
    ExtractionResponse: A generator yielding Extraction Response messages for
    each batch of extracted data.

    Example:
    request = ExtractionRequest(file_path="./data/example.zip")
    extraction_config = ExtractionWorkerConfig(
        batch_size=8,
        keep_metadata_key=["eventId"],
        keep_fluorescence_keys=\
            ["average_std", "average_mean", "relative_spectra"],
        keep_rec_properties_keys=[],
        filters={"min_area": 625, "min_solidity": 0.9}
    )
    for extraction_response in ZipExtraction(request_msg, extraction_config):
        # Process each Extraction Response message
        pass
    """
    record = zipfile.Path(request[FILE_PATH_KEY])
    index = __zip_get_index(record, POLLENO_EVENT_SUFFIX, POLLENO_REC0_SUFFIX, POLLENO_REC1_SUFFIX)
    index = __zip_filter_indexed_events(record, index, config)
    recs_generator = __zip_filtered_recs_generator(record, index)
    for batch_id, batch in enumerate(batchify(
        recs_generator, config.exw_batch_size
    )):

        _, (metadata, fluodata, rec_properties, rec0, rec1) = batch
        yield ExtractionResponse(
            file_path=request[FILE_PATH_KEY],
            batch_id=batch_id,
            metadata=metadata \
                if config.exw_keep_metadata else None,
            fluodata=fluodata \
                if config.exw_keep_fluorescence else None,
            rec_properties=rec_properties \
                if config.exw_keep_rec_properties else None,
            rec0=rec0 if config.exw_keep_rec else None,
            rec1=rec1 if config.exw_keep_rec else None
        )

def S3ZipExtraction(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs: Any
) -> Generator:
    """
    Performs extraction of events and associated recordings from a zip archive
    stored on Amazon S3.

    Parameters:
    - request (Dict): Extraction Request message containing the S3 file path to
    the zip archive.
    - config (ExtractionWorkerConfig): Configuration object specifying
    filtering and retention criteria.
    - **kwargs: Additional keyword arguments. It must contain an "s3" key
    pointing to the S3 client.

    Yields:
    ExtractionResponse: A generator yielding Extraction Response messages for
    each batch of extracted data.

    Example:
    request_msg = ExReq("s3://example_bucket/example.zip")
    extraction_config = ExtractionWorkerConfig(
        batch_size=8,
        keep_metadata_key=["eventId"],
        keep_fluorescence_keys=\
            ["average_std", "average_mean", "relative_spectra"],
        keep_rec_properties_keys=[],
        filters={"min_area": 625, "min_solidity": 0.9}
    )
    s3_client = boto3.client('s3')
    for extraction_response in S3ZipExtraction(
        request_msg,
        extraction_config,
        s3=s3_client
    ):
        # Process each Extraction Response message
        pass
    """
    if not "s3" in kwargs.keys():
        raise RuntimeError()
    
    s3_path_pattern = r"^s3:\/\/(?P<bucket>\S*?)\/(?P<key>\S*?)$"
    match = re.match(s3_path_pattern, request[FILE_PATH_KEY])
    bucket, key = match["bucket"], match["key"]

    with tempfile.NamedTemporaryFile(
        prefix=config.tmp_directory,
        suffix=".zip"
    ) as file:
        kwargs["s3"].downoad_fileobj(bucket, key, file)

        request = ExtractionRequest(file.name)
        yield from ZipExtraction(request, config, **kwargs)

def __hdf5_get_keys(record) -> List[str]:
    keys = []
    def func(name, obj):
        if isinstance(obj, h5py.Dataset):
            keys.append(name)
    record.visititems(func)
    return keys

def HDF5Extraction(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs: Any
) -> Generator:
    """
    Performs extraction of events and associated data from an HDF5 file.

    Parameters:
    - request (Dict): Extraction Request message containing the file path
    to the HDF5 file.
    - config (ExtractionWorkerConfig): Configuration object specifying batch
    size and other criteria.
    - **kwargs: Additional keyword arguments.

    Yields:
    ExtractionResponse: A generator yielding Extraction Response messages for
    each batch of extracted data.

    Example:
    request_msg = ExReq("./data/example.hdf5")
    extraction_config = ExtractionWorkerConfig(batch_size=8)
    for extraction_response in HDF5Extraction(request_msg, extraction_config):
        # Process each Extraction Response message
        pass
    """
    record = h5py.File(request[FILE_PATH_KEY])

    keys = []
    _keys = __hdf5_get_keys(record)
    if config.exw_keep_metadata:
        keys.extend([el for el in _keys if el.startswith(_METADATA_KEY)])
    if config.exw_keep_fluorescence:
        keys.extend([el for el in _keys if el.startswith(_FLUODATA_KEY)])
    if config.exw_keep_rec_properties:
        keys.extend([el for el in _keys if el.startswith(_REC_PROPERTIES_KEY)])
    if config.exw_keep_rec:
        keys.extend([el for el in _keys if el.startswith(_REC_KEY)])

    n_events = [len(record[k]) for k in keys]
    if not all([n == n_events[0] for n in n_events]):
        raise ValueError()
    n_events = n_events[0]

    for batch_id, start_slice in \
        enumerate(range(0, n_events, config.exw_batch_size)):

        data = {k: record[k][start_slice:min(start_slice+config.exw_batch_size, n_events)] \
                    for k in keys}
        data = {k: v.tolist() for k, v in data.items()}

        yield ExtractionResponse(
            file_path=request[FILE_PATH_KEY],
            batch_id=batch_id,
            **data
        )

def CSVExtraction(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs: Any
):
    record = pd.read_csv(request[FILE_PATH_KEY])

    keys = []
    _keys = record.columns
    if config.exw_keep_metadata:
        keys.extend([el for el in _keys if el.startswith(_METADATA_KEY)])
    if config.exw_keep_fluorescence:
        keys.extend([el for el in _keys if el.startswith(_FLUODATA_KEY)])
    if config.exw_keep_rec_properties:
        keys.extend([el for el in _keys if el.startswith(_REC_PROPERTIES_KEY)])
    if config.exw_keep_rec:
        keys.extend([el for el in _keys if el.startswith(_REC_KEY)])

    n_events = len(record)

    for batch_id, start_slice in \
        enumerate(range(0, n_events, config.exw_batch_size)):

        data = {k: record[k][start_slice:min(start_slice+config.exw_batch_size, n_events)] \
                    for k in keys}
        data = {k: v.to_list() for k, v in data.items()}

        yield ExtractionResponse(
            file_path=request[FILE_PATH_KEY],
            batch_id=batch_id,
            **data
        )

@PlPsCWorker
def ExtractionWorker(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs: Any
) -> Generator:
    """
    Pull-Push-Control worker function for handling extraction requests.

    Parameters:
    - request (dict): Extraction Request message dictionary.
    - config (ExtractionWorkerConfig): Configuration object.
    - **kwargs: Additional keyword arguments.

    Yields:
    ExtractionResponse: A generator yielding Extraction Response messages.

    Note:
    - It then determines the type of extraction (Zip, S3-Zip, HDF5, CSV) based
    on the file path.
    """
    if not isexreq(request):
        raise ValueError()

    if (not hass3scheme(request)) and haszipextension(request):
        yield from ZipExtraction(request, config, **kwargs)

    elif hass3scheme(request) and haszipextension(request):
        yield from S3ZipExtraction(request, config, **kwargs)

    elif (not hass3scheme(request)) and hashdf5extension(request):
        yield from HDF5Extraction(request, config, **kwargs)

    elif (not hass3scheme(request)) and hascsvextension(request):
        yield from CSVExtraction(request, config, **kwargs)

    else:
        raise NotImplementedError()
