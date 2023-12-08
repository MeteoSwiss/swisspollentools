import time
from typing import Iterable, Callable, Optional

import zmq

from swisspollentools.utils import \
    LAUNCH_SLEEP_TIME, \
    send_request, ExpectedNItems

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

    n_tasks_counter = 0
    for el in iterable:
        send_request(sender, request_fn(el, **kwargs))
        n_tasks_counter += 1

    send_request(scaffold_sender, ExpectedNItems(n_tasks_counter))

    if on_closure is not None:
        on_closure()
