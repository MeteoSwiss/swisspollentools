"""
Base workers as decorators to define new workers for HPC.
"""

import time
from typing import Callable, Optional

import zmq

from swisspollentools.utils.global_messages import *
from swisspollentools.utils.constants import *
from swisspollentools.utils.requests import *

def getPullPushChannels(
    pull_port: int,
    push_port: int,
    control_port: int
):
    """
    Set up ZeroMQ channels for Pull-Push-Control communication.

    Parameters:
    - pull_port (int): Port for the PULL connection.
    - push_port (int): Port for the PUSH connection.
    - control_port (int): Port for the control connection (PUB/SUB channel).

    Returns:
    Tuple[zmq.Context, zmq.Socket, zmq.Socket, zmq.Socket, zmq.Poller]:
        - zmq.Context: ZeroMQ context.
        - zmq.Socket: PULL connection socket.
        - zmq.Socket: PUSH connection socket.
        - zmq.Socket: Control connection (PUB/SUB channel) socket.
        - zmq.Poller: Poller for handling multiple sockets.
    """
    context = zmq.Context()

    # Set PULL connection
    receiver = context.socket(zmq.PULL)
    receiver.connect(f"tcp://127.0.0.1:{pull_port}")

    # Set PUSH connection
    sender = context.socket(zmq.PUSH)
    sender.connect(f"tcp://127.0.0.1:{push_port}")

    # Set control connection (PUB/SUB channel)
    control = context.socket(zmq.SUB)
    control.connect(f"tcp://127.0.0.1:{control_port}")
    control.setsockopt_string(zmq.SUBSCRIBE, "")

    # Group the receiver (PULL, Control)
    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(control, zmq.POLLIN)

    return context, receiver, sender, control, poller

def process_kwargs(**kwargs):
    get_kwargs = {
        k.removeprefix("get_"): v \
            for k, v in kwargs.items() \
            if k.startswith("get_")
    }
    for k, v in get_kwargs.items():
        if isinstance(v, Callable):
            get_kwargs[k] = v()
        elif isinstance(v, Tuple):
            get_kwargs[k] = v[0](*v[1:])

    kwargs = {k: v for k, v in kwargs.items() \
                if not k.startswith("get_")}
    kwargs = {**kwargs, **get_kwargs}

    return kwargs

def PullPushWorker(worker):
    """
    Decorator for creating Pull-Push worker functions.

    Returns:
    Callable: Decorator function.

    Example:
    @PullPushWorker
    def my_worker(request, config, **kwargs):
        # Worker implementation
        pass
    """
    def wrapper(
        config,
        pull_port: int,
        push_port: int,
        control_port: int,
        timeout: float=float("inf"),
        on_startup: Optional[Callable]=None,
        on_request: Optional[Callable]=None,
        on_failure: Optional[Callable]=None,
        on_closure: Optional[Callable]=None,
        **kwargs
    ) -> None:
        """
        Wrapper function for the Pull-Push worker.

        Parameters:
        - config: Configuration object.
        - pull_port (int): Port for the PULL connection.
        - push_port (int): Port for the PUSH connection.
        - control_port (int): Port for the control connection (PUB/SUB 
        channel).
        - timeout (float): Timeout for the worker loop.
        - on_startup (Callable): callable to be executed on the start-up,
        does not take any arguments
        - on_request (Callable): callable to be executed on the reception of a
        new request, takes the request as unique argument
        - on failure (Callable): callable ot be execute when an error is raised
        within the worker, takes the error as unique argument.
        - on_closure (Callable): callable to be executed on the closure, does
        not take any arguments.
        - **kwargs: Additional keyword arguments.

        Returns:
        None
        """
        if on_startup is not None:
            on_startup()

        ( context, receiver, sender,  control, poller ) = \
            getPullPushChannels(pull_port, push_port, control_port)
        time.sleep(LAUNCH_SLEEP_TIME)

        kwargs = process_kwargs(**kwargs)

        # Message listening loop
        timeout_start = time.time()
        while time.time() < timeout_start + timeout:
            socks = dict(poller.poll())

            # Pulled messages case
            if socks.get(receiver) == zmq.POLLIN:
                request = recv_request(receiver)

                if on_request is not None:
                    on_request(request)

                try:
                    for response in worker(request, config, **kwargs):
                        send_request(sender, response)
                except Exception as e:
                    if on_failure is not None:
                        on_failure(e)

                send_request(sender, EndOfTask())                 

            # Control messages case
            if socks.get(control) == zmq.POLLIN:
                # If the worker receive an EOP message, the process
                # is terminated.
                request = recv_request(control)

                if iseop(request):
                    break

        if on_closure is not None:
            on_closure()

    return wrapper

def CollateWorker(worker):
    """
    Decorator for creating Collate worker functions.

    Returns:
    Callable: Decorator function.

    Example:
    @CollateWorker
    def my_worker(request, config, **kwargs):
        # Worker implementation
        pass
    """
    def wrapper(
        config,
        pull_port: int,
        push_port: int,
        control_port: int,
        timeout: float=float("inf"),
        on_startup: Optional[Callable]=None,
        on_request: Optional[Callable]=None,
        on_failure: Optional[Callable]=None,
        on_closure: Optional[Callable]=None,
        **kwargs
    ):
        """
        Wrapper function for the Collate worker.

        Parameters:
        - config: Configuration object.
        - pull_port (int): Port for the PULL connection.
        - push_port (int): Port for the PUSH connection.
        - control_port (int): Port for the control connection (PUB/SUB 
        channel).
        - timeout (float): Timeout for the worker loop.
        - on_startup (Callable): callable to be executed on the start-up,
        does not take any arguments
        - on_request (Callable): callable to be executed on the reception of a
        new request, takes the request as unique argument
        - on failure (Callable): callable ot be execute when an error is raised
        within the worker, takes the error as unique argument.
        - on_closure (Callable): callable to be executed on the closure, does
        not take any arguments.
        - **kwargs: Additional keyword arguments.

        Returns:
        None
        """
        if on_startup is not None:
            on_startup()

        ( context, receiver, sender,  control, poller ) = \
            getPullPushChannels(pull_port, push_port, control_port)
        time.sleep(LAUNCH_SLEEP_TIME)

        kwargs = process_kwargs(**kwargs)

        # Message listening loop
        timeout_start = time.time()
        requests = []
        while time.time() < timeout_start + timeout:
            socks = dict(poller.poll())

            # Pulled messages case
            if socks.get(receiver) == zmq.POLLIN:
                request = recv_request(receiver)

                if on_request is not None:
                    on_request(request)

                requests.append(request)
                send_request(sender, EndOfTask())           

            # Control messages case
            if socks.get(control) == zmq.POLLIN:
                # If the worker receive an EOP message, the process
                # is terminated.
                request = recv_request(control)

                if iseop(request):
                    break

        try:
            response = worker(requests, config, **kwargs)
            send_request(sender, response)
        except Exception as e:
            if on_failure is not None:
                on_failure(e)

        if on_closure is not None:
            on_closure()

    return wrapper
