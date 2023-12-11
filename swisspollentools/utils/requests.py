"""
Request functions for communication using ZeroMQ.

This module provides functions for sending and receiving requests using ZeroMQ
(ZMQ). The `send_request` function is used to send a request with optional
NumPy array data, while the `recv_request` function is used to receive and
reconstruct the request.

Functions:
- send_request(socket: zmq.Socket, request: Dict, copy: bool=True, track: 
bool=False) -> None: Send a request using a ZeroMQ socket.
- recv_request(socket: zmq.Socket, copy: bool=True, track: bool=False) -> Dict:
Receive and reconstruct a request using a ZeroMQ socket.
"""

from typing import Dict

import zmq
import numpy as np

def send_request(
    socket:zmq.Socket,
    request: Dict,
    copy: bool=True,
    track: bool=False
):
    """
    Send a request using a ZeroMQ socket.

    Parameters:
    - socket (zmq.Socket): ZeroMQ socket for communication.
    - request (Dict): The request to be sent.
    - copy (bool, optional): Whether to copy the NumPy array data (default is
    True).
    - track (bool, optional): Whether to track the NumPy array data (default is
    False).

    Returns:
    None

    Example:
    # Example usage of send_request function
    import zmq

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")

    request_data = {'message': 'Hello, server!', 'image': np.array([[1, 2],
    [3, 4]])}
    send_request(socket, request_data)

    Note:
    - The function handles the transmission of both JSON data and NumPy array
    data through the ZeroMQ socket, allowing communication of complex requests.
    """
    md = {k: (str(v.dtype), v.shape) \
            for k, v in request.items() \
            if isinstance(v, np.ndarray)}
    
    if not md:
        socket.send_json(
            {k: v for k, v in request.items() \
                if not isinstance(v, np.ndarray)},
            zmq.SNDMORE
        )
        socket.send_json({})
        return
    
    socket.send_json(
        {k: v for k, v in request.items() \
            if not isinstance(v, np.ndarray)},
        zmq.SNDMORE
    )
    socket.send_json(md, zmq.SNDMORE)
    for i in range(len(md) - 1):
        key = list(md.keys())[i]
        socket.send(request[key], zmq.SNDMORE, copy=copy, track=track)
    key = list(md.keys())[-1]
    socket.send(request[key])

def recv_request(
    socket: zmq.Socket,
    copy: bool=True,
    track: bool=False
) -> Dict:
    """
    Receive and reconstruct a request using a ZeroMQ socket.

    Parameters:
    - socket (zmq.Socket): ZeroMQ socket for communication.
    - copy (bool, optional): Whether to copy the NumPy array data (default is
    True).
    - track (bool, optional): Whether to track the NumPy array data (default is
    False).

    Returns:
    Dict: The reconstructed request.

    Example:
    # Example usage of recv_request function
    import zmq

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")

    received_request = recv_request(socket)
    print(received_request)

    Note:
    - The function receives both JSON data and NumPy array data from the ZeroMQ
    socket and reconstructs the original request, allowing the handling of 
    complex requests.
    """
    request = socket.recv_json()
    md = socket.recv_json()

    for key, (dtype, shape) in md.items():
        array = socket.recv(copy=copy, track=track)
        array = np.frombuffer(memoryview(array), dtype=dtype).reshape(shape)
        request[key] = array

    return request
