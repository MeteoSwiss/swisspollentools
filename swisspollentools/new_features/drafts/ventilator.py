"""
Ventilator:
-----------
The Ventilator scaffold is used to start a pipeline by sending multiple
requests processed from an iterable. As all the SPT scaffolds, the class is
designed for HPC environment and it is not necessary nor recommended to use it
on standard devices.

Draft Implementation:
---------------------
Draft implementation of the Ventilator scaffold as a class extending the
`multiprocessing.Process` class. The goal of this implementation is to lower
the code required to instantiate a pipeline by removing imports and avoiding
creating `multiprocessing.Process` from scratch. The following interface
describes the required functions to implement scaffolds classes where ports
have to be added to the constructor. Also the start function is overwritten
from the Process' implementation to handle arguments passing.

```
from multiprocessing import Process
from typing import (
    Any, Callable, Optional
)

class ScaffoldInterface(Process):
    def __init__(
        context: Optional[zmq.Context]=None, 
        on_startup: Optional[Callable]=None,
        on_closure: Optional[Callable]=None
    ) -> None:
        pass

    def set_sockets(self) -> None:
        pass

    def start(self, *args: Any, **kwargs: Any) -> None:
        super().start(target=self.run, args=args, kwargs=kwargs)

    def run(self, *args: Any, **kwargs: Any) -> None:
        pass
```
"""

import time
from multiprocessing import Process
from typing import (
    Any,
    Callable,
    Iterable,
    Optional
)

import zmq

from swisspollentools.utils import (
    LAUNCH_SLEEP_TIME,
    send_request,
    ExpectedNItems
)

class Ventilator(Process):
    """
    Ventilator Process for starting a pipeline from an iterable and a request
    function

    Parameters:
    ----------
    - context (`zmq.Context`): ZeroMQ context manager.
    - sender (`zmq.Socket`): ZeroMQ socket spawned from the context to emit
    requests to the next pipeline element.
    - scaffold_sender (`zmq.Socket`): ZeroMQ socket spawned from the context to
    emit a message with the number of emited requests to the next scaffold.
    - ports (`dict`): Dictionnary specifying the ports used by the scaffold:
        - sender: Port for the sender socket
        - scaffold_sender: Port for the scaffold_sender socket
    - on_startup (`Callable`): An optional callback function to execute on the
    scaffold startup.
    - on_closure (`Callable`): An optional callback function to execute on the
    scaffold closure.

    Example:
    --------
    ```
    ventilator = Ventilator(
        push_port=5555,
        scaffold_port=5556,
        on_startup=lambda: \\
            print("Ventilator scaffold started", flush=True),
        on_closure=lambda: \\
            print("Ventilator scaffold closed", flush=True)
    )

    elements = [
        # TODO: define the elements to send to the ventilator
        # ...
    ]
    def request_fn(element):
        # TODO: define the request function, it should return 
        # a dictionnary
        # ...

    ventilator.start(elements, request_fn)
    ```

    Note:
    -----
    - The Ventilator scaffold creates ZeroMQ sockets for communication.
    - Logging messages can be written within the `on_startup` and `on_closure`
    functions.
    """
    __slots__ = (
        "context", "sender", "scaffold_sender", "ports", "on_startup",
        "on_closure"
    )

    def __init__(
        self,
        push_port: int,
        scaffold_port: int,
        context: Optional[zmq.Context]=None, 
        on_startup: Optional[Callable]=None,
        on_closure: Optional[Callable]=None,
    ):
        """
        Ventilator Process constructor

        Arguments:
        -----------
        - push_port (`int`): The port for sending the requests to the next
        pipeline element.
        - scaffold_port (`int`): The port for sending the number of tasks
        emitted to the next scaffold.
        - context (`zmq.Context`): ZeroMQ context manager. If not specified, a
        new context instance is generated.
        - on_startup (`Callable`): An optional callback function to execute
        on the scaffold startup.
        - on_closure (`Callable`): An optional callback function to execute
        on the scaffold closure.

        Returns:
        --------
        - None

        Example:
        --------
        ```
        ventilator = Ventilator(
            push_port=8888,
            scaffold_port=8889,
            on_startup=lambda: \\
                print("Ventilator scaffold started.", flush=True),
            on_closure=lambda: \\
                print("Ventilator scaffold closed.", flush=True)
        )
        ```
        """
        if not context:
            context = zmq.Context.instance()

        self.context = context
        self.ports = {"sender": push_port, "scaffold_sender": scaffold_port}
        self.on_startup = on_startup
        self.on_closure = on_closure

    def set_sockets(self) -> None:
        """
        Socket setter

        Returns:
        --------
        - None
        """        
        self.sender: zmq.Socket = self.context.socket(zmq.PUSH)
        self.sender.bind(
            f"tcp://127.0.0.1:{self.ports['sender']}"
        )

        self.scaffold_sender: zmq.Socket = self.context.socket(zmq.PAIR)
        self.scaffold_sender.bind(
            f"tcp://127.0.0.1:{self.ports['scaffold_sender']}"
        )

    def start(self, *args: Any, **kwargs: Any) -> None:
        """
        Start child process

        Arguments:
        ----------
        - Inherits from the `run` method arguments.

        Returns:
        --------
        - None

        Example:
        --------
        ```
        ventilator = Ventilator(
            # TODO: define the constructor arguments
            # ...
        )

        elements = [
            # TODO: define the elements to send to the ventilator
            # ...
        ]
        def request_fn(element):
            # TODO: define the request function, it should return 
            # a dictionnary
            # ...

        ventilator.start(elements, request_fn)
        ```
        """
        super().start(target=self.run, args=args, kwargs=kwargs)

    def run(
        self,
        iterable: Iterable,
        request_fn: Callable,
        **kwargs: Any
    ) -> None:
        """
        Execute the ventilator on a list of requests, processed from the
        iterable argument using the request function

        Arguments:
        ----------
        - iterable (`Iterable`): List of elements to be converted as requests.
        - request_fn (`Callable`): Function processings the elements inside the
        iterable argument into requests.

        Returns:
        --------
        - None

        Note:
        -----
        - The run function is automatically called using the start function and
        thus does not require to be called directly.
        """
        if self.on_startup is not None:
            self.on_startup()

        # Set the sockets and add a waiting time (soft way to ensure all the
        # pipeline's scaffolds and workers have been started)
        self.set_sockets()
        time.sleep(LAUNCH_SLEEP_TIME)

        # Loop to emit the requests and count the number of emitted requests
        n_tasks_emitted = 0
        for el in iterable:
            send_request(self.sender, request_fn(el, **kwargs))
            n_tasks_emitted += 1

        # Emit the expected number of items message to the next scaffold
        send_request(self.scaffold_sender, ExpectedNItems(n_tasks_emitted))

        if self.on_closure is not None:
            self.on_closure()
