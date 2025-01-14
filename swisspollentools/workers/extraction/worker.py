"""
SwissPollenTools Extraction Worker

The `worker.py` module defines the Extraction Worker, which is responsible for
handling extraction requests. It supports extraction from various file formats
such as ZIP archives, HDF5 files, and CSV files.

Functions:
----------
- `ZipExtraction(request: Dict, config: ExtractionWorkerConfig, **kwargs) -> 
Generator`: Performs extraction from a ZIP archive.
- `S3ZipExtraction(request: Dict, config: ExtractionWorkerConfig, **kwargs) -> 
Generator`: Performs extraction from a ZIP archive stored on Amazon S3.
- `HDF5Extraction(request: Dict, config: ExtractionWorkerConfig, **kwargs) -> 
Generator`: Performs extraction from an HDF5 file.
- `CSVExtraction(request: Dict, config: ExtractionWorkerConfig, **kwargs) -> 
Generator`: Performs extraction from a CSV file.
- `ExtractionWorker(request: Dict, config: ExtractionWorkerConfig, **kwargs) -> 
Generator`: Main worker function for handling extraction requests.
"""
import re

from io import BytesIO

import tempfile
import zipfile
import json

from typing import Dict, Generator, List, Tuple

import h5py
import pandas as pd

from PIL import Image
import numpy as np

from swisspollentools.workers.extraction.config import \
    FILTER_PREFIX, ExtractionWorkerConfig
from swisspollentools.workers.extraction.messages import \
    ExtractionResponse, ExtractionRequest, \
    isexreq, hass3scheme, haszipextension, hashdf5extension, hascsvextension
from swisspollentools.schemas import auto_caster
from swisspollentools.utils import \
    FILE_PATH_KEY, VALID_POLENO_EVENT_SUFFIX, \
    VALID_POLENO_REC0_SUFFIX, VALID_POLENO_REC1_SUFFIX, \
    PullPushWorker, batchify
from swisspollentools.utils.constants import \
    _METADATA_KEY, _FLUODATA_KEY, _REC_PROPERTIES_KEY, \
    _REC_KEY, _LABEL_KEY, _NP_ARRAY_DATA_KEYS

def __zip_get_suffix(
    record: zipfile.Path,
    candidates: List[str]
) -> str:
    """
    Select a suffix from the candidates that match the record.

    Parameters:
    -----------
    - record (zipfile.Path): The zipfile.Path object representing the zip 
    archive.
    - candidates (list[str]): The list of suffix candidates

    Returns:
    --------
    - str: The matching suffix

    Raise:
    ------
    - ValueError: no matching suffix found or multiple matching suffix found
    """
    suffix = {
        suffix for suffix in candidates \
            if any(el.name.endswith(suffix) for el in record.iterdir())
        }
    if len(suffix) != 1:
        raise ValueError(f"Expected one valid candidate, found {len(suffix)}")

    return list(suffix)[0]

def __zip_get_suffixes(
    record: zipfile.Path
) -> Tuple[str, str, str]:
    """
    Generate a tuple with the valid JSON event suffix and rec0, rec1 suffixes

    Parameters:
    -----------
    - record (zipfile.Path): The zipfile.Path object representing the zip 
    archive.

    Returns:
    --------
    - tuple(str, str, str): The event, rec0 and rec1 suffixes matching the
    record
    """
    poleno_event_suffix = __zip_get_suffix(record, VALID_POLENO_EVENT_SUFFIX)
    poleno_rec0_suffix = __zip_get_suffix(record, VALID_POLENO_REC0_SUFFIX)
    poleno_rec1_suffix = __zip_get_suffix(record, VALID_POLENO_REC1_SUFFIX)

    return poleno_event_suffix, poleno_rec0_suffix, poleno_rec1_suffix

def __zip_get_index(
    record: zipfile.Path,
    *args: List[str]
) -> List[str]:
    """
    Generates an index of file names within a given zipfile. Path based on
    specified patterns.

    Parameters:
    -----------
    - record (zipfile.Path): The zipfile.Path object representing the zip 
    archive.
    - *args (List[str]): Variable number of string arguments representing 
    patterns to match file names.

    Returns:
    --------
    - List[str]: A list of unique file names within the zip archive that match 
    all specified patterns.

    Note:
    -----
    - File names are obtained by removing the specified patterns from the end
    of the file names.
    """
    def index_fn(pattern):
        return {
            el.name.removesuffix(pattern) \
                for el in record.iterdir() \
                if el.name.endswith(pattern)
        }

    index = set.intersection(*[index_fn(arg) for arg in args if isinstance(arg, str)])
    return list(index)

def __zip_read_event(
    record: zipfile.Path,
    event_id: str,
    suffix: str,
    keep_metadata_key: List[str]=[],
    keep_fluorescence_keys: List[str]=[],
    keep_rec_properties_keys: List[str]=[]
) -> Tuple[dict, dict, Tuple[dict, dict]]:
    """
    Reads and extracts relevant information from an event within a zip archive.

    Parameters:
    -----------
    - record (zipfile.Path): The zipfile.Path object representing the zip 
    archive.
    - event_id (str): The identifier of the event to be read.
    - suffix (str): The suffix for the JSON event file.
    - keep_metadata_key (List[str], optional): List of metadata keys to retain,
    default is an empty list.
    - keep_fluorescence_keys (List[str], optional): List of fluorescence data
    keys to retain, default is an empty list.
    - keep_rec_properties_keys (List[str], optional): List of recording
    properties keys to retain, default is an empty list.

    Returns:
    --------
    - Tuple[dict, dict, Tuple[dict, dict]]: A tuple containing:
        - A dictionary representing the metadata of the event.
        - A dictionary representing the fluorescence data of the event.
        - A tuple containing two dictionaries representing recording properties
        for two images (rec0 and rec1).
    """
    event = record.joinpath(event_id + suffix).read_bytes()
    event = json.loads(event)
    event = auto_caster(event)

    metadata = event["metaData"].schema
    fluorescence_data = event["fluoData"].schema
    rec0_properties = event["recProperties"][0].schema
    rec1_properties = event["recProperties"][1].schema

    if keep_metadata_key:
        metadata = {k: metadata[k] for k in keep_metadata_key}
    if keep_fluorescence_keys:
        fluorescence_data = {k: np.array(fluorescence_data[k]) \
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
    event_id: str,
    suffix: str
) -> np.ndarray:
    """
    Reads and flattens an image file within a zip archive.

    Parameters:
    -----------
    - record (zipfile.Path): The zipfile.Path object representing the zip 
    archive.
    - id (str): The identifier of the recording to be read.
    - suffix (str): The suffix indicating the type of recording file (e.g., 
    POLLENO_REC0_SUFFIX, POLLENO_REC1_SUFFIX).

    Returns:
    --------
    - np.ndarray: A flattened NumPy array representing the pixel values of the image.
    """
    rec = BytesIO(record.joinpath(event_id + suffix).read_bytes())
    rec = np.array(Image.open(rec))

    return rec

def __zip_filter_indexed_events(
    record: zipfile.Path,
    index: List[str],
    suffix: str,
    config: ExtractionWorkerConfig
) -> Generator:
    """
    Filters and yields events from a zip archive based on a given index and
    configuration.

    Parameters:
    -----------
    - record (zipfile.Path): The zipfile.Path object representing the zip
    archive.
    - index (List[str]): List of event IDs to filter and process.
    - suffix (str): The suffix for the JSON event file.
    - config (ExtractionWorkerConfig): Configuration object specifying
    filtering and retention criteria.

    Yields:
    -------
    - Tuple[str, Tuple[dict, dict, Tuple[dict, dict]]]: A tuple containing:
        - The event ID.
        - A tuple representing the filtered metadata, fluorescence data, and
        recording properties for two images (rec0 and rec1).

    Note:
    -----
    - It applies filters specified in the config object to exclude events based
    on recording properties.
    """
    for event_id in index:
        event = __zip_read_event(
            record=record,
            event_id=event_id,
            suffix=suffix,
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

        yield event_id, (metadata, fluodata, rec_properties)

def __zip_filtered_recs_generator(
    record: zipfile.Path,
    index: Generator,
    suffixes: Tuple[str, str]
) -> Generator:
    """
    Generates and yields filtered recordings from a zip archive based on a
    given index.

    Parameters:
    -----------
    - record (zipfile.Path): The zipfile.Path object representing the zip
    archive.
    - index (typing.Generator): Generator yielding tuples with event ID and
    associated data.
    - suffixes (Tuple[str, str]): Tuple containing the suffixes for rec0 and
    rec1.

    Yields:
    -------
    - Tuple[str, dict, dict, Tuple[dict, dict], np.ndarray, np.ndarray]: A
    tuple containing:
        - The event ID.
        - Metadata dictionary.
        - Fluorescence data dictionary.
        - Tuple of recording properties for two images (rec0 and rec1).
        - NumPy array representing flattened recording data for rec0.
        - NumPy array representing flattened recording data for rec1.

    Note:
    -----
    - Expects the index to yield tuples with event ID and associated data,
    including metadata, fluorescence, and rec_properties.
    - Yields tuples containing event ID, metadata, fluorescence data, recording 
    properties, and flattened recording data.
    """
    for event_id, data in index:
        rec0 = __zip_read_rec(record, event_id, suffixes[0])
        rec1 = __zip_read_rec(record, event_id, suffixes[1])

        yield event_id, (*data, rec0, rec1)

def ZipExtraction(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs
) -> Generator:
    """
    Performs extraction of events and associated recordings from a zip archive.

    Parameters:
    -----------
    - request (Dict): Extraction Request message containing the file path to
    the zip archive.
    - config (ExtractionWorkerConfig): Configuration object specifying
    filtering and retention criteria.
    - **kwargs: Additional keyword arguments.

    Yields:
    -------
    ExtractionResponse: A generator yielding Extraction Response messages for
    each batch of extracted data.

    Example:
    --------
    ```
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
    ```
    """
    record = zipfile.Path(request[FILE_PATH_KEY])
    event_suffix, rec0_suffix, rec1_suffix = __zip_get_suffixes(record)
    index = __zip_get_index(
        record,
        event_suffix,
        rec0_suffix,
        rec1_suffix
    )
    index = __zip_filter_indexed_events(
        record,
        index,
        event_suffix,
        config
    )
    recs_generator = __zip_filtered_recs_generator(
        record,
        index,
        (rec0_suffix, rec1_suffix)
    )
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
    **kwargs
) -> Generator:
    """
    Performs extraction of events and associated recordings from a zip archive
    stored on Amazon S3.

    Parameters:
    -----------
    - request (Dict): Extraction Request message containing the S3 file path to
    the zip archive.
    - config (ExtractionWorkerConfig): Configuration object specifying
    filtering and retention criteria.
    - **kwargs: Additional keyword arguments. It must contain an "s3" key
    pointing to the S3 client.

    Yields:
    -------
    ExtractionResponse: A generator yielding Extraction Response messages for
    each batch of extracted data.

    Example:
    --------
    ```
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
    ```
    """
    if "s3" not in kwargs:
        raise RuntimeError()

    s3_path_pattern = r"^s3:\/\/(?P<bucket>\S*?)\/(?P<key>\S*?)$"
    match = re.match(s3_path_pattern, request[FILE_PATH_KEY])
    bucket, key = match["bucket"], match["key"]

    with tempfile.NamedTemporaryFile(
        prefix=config.exw_tmp_directory,
        suffix=".zip"
    ) as file:
        kwargs["s3"].download_fileobj(bucket, key, file)

        request = ExtractionRequest(file.name)
        yield from ZipExtraction(request, config, **kwargs)

def __hdf5_get_keys(record: h5py.File) -> List[str]:
    """
    Returns the list of keys in an HDF5 record

    Parameters:
    -----------
    - record (h5py.File): The HDF5 file containing the data

    Returns:
    --------
    - List[str]: the list of keys in the HDF5 record
    """
    keys = []
    def func(name, obj):
        if isinstance(obj, h5py.Dataset):
            keys.append(name)
    record.visititems(func)
    return keys

def HDF5Extraction(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs
) -> Generator:
    """
    Performs extraction of events and associated data from an HDF5 file.

    Parameters:
    -----------
    - request (Dict): Extraction Request message containing the file path
    to the HDF5 file.
    - config (ExtractionWorkerConfig): Configuration object specifying batch
    size and other criteria.
    - **kwargs: Additional keyword arguments.

    Yields:
    -------
    - ExtractionResponse: A generator yielding Extraction Response messages for
    each batch of extracted data.

    Example:
    --------
    ```
    request_msg = ExReq("./data/example.hdf5")
    extraction_config = ExtractionWorkerConfig(batch_size=8)
    for extraction_response in HDF5Extraction(request_msg, extraction_config):
        # Process each Extraction Response message
        pass
    ```
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
    if config.exw_keep_label:
        keys.extend([el for el in _keys if el.startswith(_LABEL_KEY)])

    n_events = [len(record[k]) for k in keys]
    if not all(n == n_events[0] for n in n_events):
        raise ValueError()
    n_events = n_events[0]

    for batch_id, start_slice in \
        enumerate(range(0, n_events, config.exw_batch_size)):

        data = {k: record[k][start_slice:min(start_slice+config.exw_batch_size, n_events)] \
                    for k in keys}
        data = {k: v.tolist() if not k.startswith(_NP_ARRAY_DATA_KEYS) \
                    else v \
                    for k, v in data.items()}

        yield ExtractionResponse(
            file_path=request[FILE_PATH_KEY],
            batch_id=batch_id,
            **data
        )

def CSVExtraction(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs
):
    """
    Performs extraction of events and associated data from a CSV file.

    Parameters:
    -----------
    - `request` (Dict): Extraction Request message containing the file path
      to the CSV file.
    - `config` (ExtractionWorkerConfig): Configuration object specifying batch
      size and other criteria.
    - `**kwargs`: Additional keyword arguments.

    Yields:
    -------
    - `ExtractionResponse`: A generator yielding Extraction Response messages
    for each batch of extracted data.

    Example:
    --------
    ```
    request_msg = ExReq("./data/example.csv")
    extraction_config = ExtractionWorkerConfig(batch_size=8)
    for extraction_response in CSVExtraction(request_msg, extraction_config):
        # Process each Extraction Response message
        pass
    ```

    Notes:
    ------
    - The CSV file is expected to have columns representing different types of
    data.
    - The function reads the CSV file, filters and organizes the data based on
    the specified criteria in the `config` object, and yields batches of
    `ExtractionResponse` messages.

    Parameters in `ExtractionResponse`:
    -----------------------------------
    - `file_path` (str): The path to the CSV file.
    - `batch_id` (int): The ID of the current batch.
    - Additional parameters based on the configuration such as `metadata`,
    `fluodata`, `rec_properties`, `rec0`, `rec1`, etc., depending on what is
    specified to be kept.
    """
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
    if config.exw_keep_label:
        keys.extend([el for el in _keys if el.startswith(_LABEL_KEY)])

    n_events = len(record)

    for batch_id, start_slice in \
        enumerate(range(0, n_events, config.exw_batch_size)):

        data = {k: record[k][start_slice:min(start_slice+config.exw_batch_size, n_events)] \
                    for k in keys}
        data = {
            k: v.to_list() if not k.startswith(_NP_ARRAY_DATA_KEYS) \
                else np.stack(v.apply(lambda el: np.array(eval(el))).to_list(), axis=0) \
                for k, v in data.items()
        }

        yield ExtractionResponse(
            file_path=request[FILE_PATH_KEY],
            batch_id=batch_id,
            **data
        )

@PullPushWorker
def ExtractionWorker(
    request: Dict,
    config: ExtractionWorkerConfig,
    **kwargs
) -> Generator:
    """
    Pull-Push-Control worker function for handling extraction requests.

    Parameters:
    -----------
    - request (dict): Extraction Request message dictionary.
    - config (ExtractionWorkerConfig): Configuration object.
    - **kwargs: Additional keyword arguments.

    Yields:
    -------
    ExtractionResponse: A generator yielding Extraction Response messages.

    Note:
    -----
    - It determines the type of extraction (Zip, S3-Zip, HDF5, CSV) based
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
