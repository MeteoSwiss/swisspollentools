import zmq
import numpy as np
from typing import Dict

from swisspollentools.utils.global_messages import *

def send_request(
    socket:zmq.Socket,
    request: Dict,
    copy: bool=True,
    track: bool=False
):
    md = {k: (str(v.dtype), v.shape) for k, v in request.items() if isinstance(v, np.ndarray)}
    
    if not md:
        socket.send_json({k: v for k, v in request.items() if not isinstance(v, np.ndarray)}, zmq.SNDMORE)
        socket.send_json({})
        return
    
    socket.send_json({k: v for k, v in request.items() if not isinstance(v, np.ndarray)}, zmq.SNDMORE)
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
    request = socket.recv_json()
    md = socket.recv_json()

    for key, (dtype, shape) in md.items():
        array = socket.recv(copy=copy, track=track)
        array = np.frombuffer(memoryview(array), dtype=dtype).reshape(shape)
        request[key] = array

    return request
