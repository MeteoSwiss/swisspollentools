import time

import zmq

from typing import Iterable, Callable, Optional

from swisspollentools.utils import *

def Ventilator(
    iterable: Iterable,
    request_fn: Callable,
    push_port: int,
    scaffold_port: int,
    on_startup: Optional[Callable]=None,
    on_closure: Optional[Callable]=None,
    **kwargs
):
    if on_startup is not None:
        on_startup()

    context = zmq.Context()

    # Set PUSH binding
    sender = context.socket(zmq.PUSH)
    sender.bind(f"tcp://127.0.0.1:{push_port}")

    scaffold_sender = context.socket(zmq.PAIR)
    scaffold_sender.bind(f"tcp://127.0.0.1:{scaffold_port}")

    time.sleep(LAUNCH_SLEEP_TIME)

    for i, el in enumerate(iterable, 1):
        sender.send_json(request_fn(el, **kwargs))

    scaffold_sender.send_json(ExpectedNItems(i))

    if on_closure is not None:
        on_closure()

    return
