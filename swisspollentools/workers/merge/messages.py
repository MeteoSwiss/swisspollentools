"""
Merge Worker Messages: messages.py

This module defines the messages used by the Merge Worker for communication
between
components.

Functions:
- MergeRequest: Generates a merge request message.
- MergeResponse: Generates a merge response message.
- ismerreq: Checks if a given dictionary represents a merge request.
- ismerrep: Checks if a given dictionary represents a merge response.
- parsemerreq: Parses a merge request dictionary and returns relevant 
components.
- parsemerrep: Parses a merge response dictionary and returns relevant
components.
"""
from typing import Any, Dict

from swisspollentools.utils import *

def MergeRequest(
    file_path: str,
    batch_id: Any=None,
    response: Dict={}
) -> Dict:
    """
    Generates a merge request message.

    Parameters:
    - file_path (str): The file path for the input data to be merged.

    Returns:
    Dict[str, Any]: A dictionary representing the merge request message.

    Example:
    merge_request = MergeRequest(file_path="./data/input_data.spt")
    """
    if not ismsg(response):
        raise ValueError()

    msg = {REQUEST_TYPE_KEY: MERGE_REQUEST_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id
    msg[BODY_KEY] = get_body(response)

    msg = flatten_dictionary(msg)
    return msg

def MeReq(*args, **kwargs) -> Dict:
    """
    Alias for MergeRequest
    """
    return MergeRequest(*args, **kwargs)

@assert_ismsg
def ismereq(msg: Dict) -> bool: 
    """
    Returns True if the message is a Merge Request message.

    Parameters:
    - msg (dict): Message dictionary.
    """  
    return msg[REQUEST_TYPE_KEY] == MERGE_REQUEST_VALUE

def MergeResponse(
    file_path: str,
    batch_id: Any=None,
    *args,
    **kwargs
) -> Dict:
    """
    Generates a merge response message.

    Parameters:
    - file_path (str): The file path for the merged data.
    - success (bool): Indicates whether the merge operation was successful.
      Default is True.

    Returns:
    Dict[str, Any]: A dictionary representing the merge response message.
    """
    msg = {REQUEST_TYPE_KEY: MERGE_RESPONSE_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id

    for k, v in enumerate(args):
        k = KEY_SEP.join([BODY_KEY, str(k)])
        msg[k] = v

    for k, v in kwargs.items():
        k = KEY_SEP.join([BODY_KEY, k])
        msg[k] = v

    msg = flatten_dictionary(msg)
    return msg

def MeRes(*args, **kwargs) -> Dict:
    """
    Alias for MergeResponse.
    """
    return MergeResponse(*args, **kwargs)

@assert_ismsg
def ismeres(msg: Dict) -> bool:
    """
    Returns True if the message is a Merge Response message.

    Parameters:
    - msg (dict): Message dictionary.
    """
    return msg[REQUEST_TYPE_KEY] == MERGE_RESPONSE_VALUE
