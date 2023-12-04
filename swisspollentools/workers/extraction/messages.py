from typing import Any, Dict

from swisspollentools.utils import *

def ExtractionRequest(file_path: str) -> Dict:
    """
    Creates an Extraction Request message.

    Parameters:
    - file_path (str): The file path to be extracted.

    Returns:
    dict: Extraction Request message.
    """
    msg = {REQUEST_TYPE_KEY: EXTRACTION_REQUEST_VALUE}
    msg[FILE_PATH_KEY] = file_path

    msg = flatten_dictionary(msg)
    return msg

def ExReq(*args: Any, **kwargs: Any) -> Dict:
    """
    Alias for ExtractionRequest.
    """
    return ExtractionRequest(*args, **kwargs)

def isexreq(msg: Dict) -> bool:
    """
    Returns True if the message is an Extraction Request message.

    Parameters:
    - msg (dict): Message dictionary.

    Raises:
    - ValueError: If called on a non-message dictionary.
    """
    if not ismsg(msg):
        raise ValueError("Calling `isexreq` on non-message dictionnary")
    
    return msg[REQUEST_TYPE_KEY] == EXTRACTION_REQUEST_VALUE

def __hasscheme(msg: Dict, scheme: str) -> bool:
    """
    Returns True if the message file path starts with the given scheme.

    Parameters:
    - msg (dict): Message dictionary.
    - scheme (str): The scheme to check.

    Raises:
    - ValueError: If called on a non-Extraction Request dictionary.
    """
    if not isexreq(msg):
        raise ValueError(
            "Calling `__hasscheme` on non Extraction Request dictionnary"
        )

    if not scheme.endswith("://"):
        scheme = scheme + "://"

    return msg[FILE_PATH_KEY].startswith(scheme)

def hass3scheme(msg: Dict) -> bool:
    """
    Returns True if the message file path starts with the 's3://' scheme.

    Parameters:
    - msg (dict): Message dictionary.
    """
    return __hasscheme(msg, "s3://")

def __hasextension(msg: Dict, extension: str) -> bool:
    """
    Returns True if the message file path ends with the given extension.

    Parameters:
    - msg (dict): Message dictionary.
    - extension (str): The extension to check.

    Raises:
    - ValueError: If called on a non-Extraction Request dictionary.
    """
    if not isexreq(msg):
        raise ValueError(
            "Calling `__hasextension` on non Extraction Request dictionnary"
        )

    if not extension.startswith("."):
        extension = "." + extension

    return msg[FILE_PATH_KEY].endswith(extension)

def haszipextension(msg: Dict) -> bool:
    """
    Returns True if the message file path ends with the '.zip' extension.

    Parameters:
    - msg (dict): Message dictionary.
    """
    return __hasextension(msg, ".zip")

def hashdf5extension(msg: Dict) -> bool:
    """
    Returns True if the message file path ends with the '.hdf5' extension.

    Parameters:
    - msg (dict): Message dictionary.
    """
    return __hasextension(msg, ".hdf5")

def hascsvextension(msg: Dict) -> bool:
    """
    Returns True if the message file path ends with the '.csv' extension.

    Parameters:
    - msg (dict): Message dictionary.
    """
    return __hasextension(msg, ".csv") 

def ExtractionResponse(
    file_path: str,
    batch_id: Any,
    metadata=None,
    fluodata=None,
    rec_properties=None,
    rec0=None,
    rec1=None,
    *args: Any,
    **kwargs: Any
) -> Dict:
    """
    Creates an Extraction Response message.

    Parameters:
    - request (dict): The original Extraction Request message.
    - batch_id (str): The batch ID associated with the extraction.
    - metadata (dict, optional): Metadata dictionary, default is None.
    - fluodata (dict, optional): Fluorescence data dictionary, default is None.
    - rec_properties (Tuple[dict, dict], optional): Tuple of recording
    properties for rec0 and rec1, default is None.
    - rec0 (np.ndarray, optional): NumPy array representing flattened recording
    data for rec0, default is None.
    - rec1 (np.ndarray, optional): NumPy array representing flattened recording
    data for rec1, default is None.

    Returns:
    dict: Extraction Response message.
    """
    msg = {REQUEST_TYPE_KEY: EXTRACTION_RESPONSE_VALUE}
    msg[FILE_PATH_KEY] = file_path
    msg[BATCH_ID_KEY] = batch_id

    if metadata is not None:
        msg[METADATA_KEY] = metadata

    if fluodata is not None:
        msg[FLUODATA_KEY] = fluodata
    
    if rec_properties is not None:
        msg[REC0_PROPERTIES_KEY], msg[REC1_PROPERTIES_KEY] = rec_properties

    if rec0 is not None:
        msg[REC0_KEY] = rec0
    
    if rec1 is not None:
        msg[REC1_KEY] = rec1
    
    for k, v in enumerate(args):
        k = KEY_SEP.join([BODY_KEY, str(k)])
        msg[k] = v

    for k, v in kwargs.items():
        k = KEY_SEP.join([BODY_KEY, k])
        msg[k] = v
    
    msg = flatten_dictionary(msg)
    return msg

def ExRes(*args: Any, **kwargs: Any) -> Dict:
    """
    Alias for ExtractionResponse.
    """
    return ExtractionResponse(*args, **kwargs)

def isexres(msg: Dict) -> bool:
    """
    Returns True if the message is an Extraction Response message.

    Parameters:
    - msg (dict): Message dictionary.

    Raises:
    - ValueError: If called on a non-message dictionary.
    """
    if not ismsg(msg):
        raise ValueError("Calling `isexres` on non-message dictionnary")
    
    return msg[REQUEST_TYPE_KEY] == EXTRACTION_RESPONSE_VALUE