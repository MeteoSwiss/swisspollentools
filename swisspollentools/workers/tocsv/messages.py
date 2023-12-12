"""
ToCSV Worker Messages: messages.py

This module defines the message creation functions for the ToCSV Worker,
including request and response message creation, message type checking
functions, and aliases.

Message Functions:
- ToCSVRequest: Creates a ToCSV Worker request message.
- ToCSVReq: Alias for ToCSVRequest function.
- istocsvreq: Checks if a message is a ToCSV Worker request.
- ToCSVResponse: Creates a ToCSV Worker response message.
- ToCSVRes: Alias for ToCSVResponse function.
- istocsvres: Checks if a message is a ToCSV Worker response.
"""
from typing import Any, Dict, List, Tuple

from swisspollentools.utils import *

def ToCSVRequest(
    file_path: str,
    batch_id: Any=None,
    response: Dict={}
):
    """
    ToCSV Worker Request Message Creation

    Parameters:
    - file_path (str): The file path for the CSV output.
    - batch_id (Any, optional): The batch identifier. Defaults to None.
    - response (Dict, optional): The response dictionary to include in the
    request. Defaults to {}.

    Returns:
    Dict: The ToCSV Worker request message.

    Raises:
    - ValueError: If the response is not a valid message dictionary.

    """
    if not ismsg(response):
        raise ValueError()
    
    msg = {REQUEST_TYPE_KEY: TOCSV_REQUEST_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id
    msg[BODY_KEY] = get_body(response)

    msg = flatten_dictionary(msg)
    return msg

def ToCSVReq(*args, **kwargs) -> Dict:
    """
    Alias for ToCSVRequest
    """
    return ToCSVRequest(*args, **kwargs)

@assert_ismsg
def istocsvreq(msg: Dict) -> bool:
    """
    Checks if a message is a ToCSV Worker request.

    Parameters:
    - msg (Dict): The message dictionary.

    Returns:
    bool: True if the message is a ToCSV Worker request, False otherwise.

    """
    return msg[REQUEST_TYPE_KEY] == TOCSV_REQUEST_VALUE

def ToCSVResponse(
    file_path: str,
    batch_id: Any=None,
    *args,
    **kwargs
):
    """
    ToCSV Worker Response Message Creation

    Parameters:
    - file_path (str): The file path for the CSV output.
    - batch_id (Any, optional): The batch identifier. Defaults to None.
    - *args, **kwargs: Additional data to include in the response.

    Returns:
    Dict: The ToCSV Worker response message.
    """
    msg = {REQUEST_TYPE_KEY: TOCSV_RESPONSE_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id

    msg = flatten_dictionary(msg)
    return msg

def ToCSVRes(*args, **kwargs):
    """
    Alias for ToCSVResponse
    """
    return ToCSVResponse(*args, **kwargs)

@assert_ismsg
def istocsvres(msg: Dict) -> bool:
    """
    Checks if a message is a ToCSV Worker response.

    Parameters:
    - msg (Dict): The message dictionary.

    Returns:
    bool: True if the message is a ToCSV Worker response, False otherwise.
    """
    return msg[REQUEST_TYPE_KEY] == TOCSV_RESPONSE_VALUE
