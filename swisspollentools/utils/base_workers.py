import time

import zmq

from typing import Callable, Optional

from swisspollentools.utils.global_messages import *
from swisspollentools.utils.constants import *

def getPlPsCChannels(
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

def PlPsCWorker(worker):
    """
    Decorator for creating Pull-Push-Control worker functions.

    Parameters:
    - on_eot (str): Action to take when receiving an EndOfTask (EOT) message.
    Options: "ignore", "forward".
    - send_eot (bool): Whether to send an EOT message after processing.

    Returns:
    Callable: Decorator function.

    Example:
    @PlPsCWorker
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
        on_closure: Optional[Callable]=None,
        **kwargs
    ) -> None:
        """
        Wrapper function for the Pull-Push-Control worker.

        Parameters:
        - config: Configuration object.
        - pull_port (int): Port for the PULL connection.
        - push_port (int): Port for the PUSH connection.
        - control_port (int): Port for the control connection (PUB/SUB 
        channel).
        - timeout (float): Timeout for the worker loop.
        - **kwargs: Additional keyword arguments.

        Returns:
        None
        """
        if on_startup is not None:
            on_startup()

        ( context, receiver, sender,  control, poller ) = \
            getPlPsCChannels(pull_port, push_port, control_port)
        time.sleep(LAUNCH_SLEEP_TIME)

        # Message listening loop
        timeout_start = time.time()
        while time.time() < timeout_start + timeout:
            socks = dict(poller.poll())

            # Pulled messages case
            if socks.get(receiver) == zmq.POLLIN:
                request = receiver.recv_json()

                if on_request is not None:
                    on_request(request)

                for response in worker(request, config, **kwargs):
                    sender.send_json(response)

                sender.send_json(EndOfTask())                        

            # Control messages case
            if socks.get(control) == zmq.POLLIN:
                # If the worker receive an EOP message, the process
                # is terminated.
                request = control.recv_json()

                if iseop(request):
                    break

        if on_closure is not None:
            on_closure()

        return

    return wrapper
