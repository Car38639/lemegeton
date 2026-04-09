import threading
from typing import Any, Dict, Optional

import zmq


class MetadataHost:
    """ZMQ REP socket to respond with metadata upon request."""

    def __init__(self, context: Optional[zmq.Context] = None, port: int = 60000):
        """
        Args:
            metadata: The metadata to send in response to requests.
            host: Host/IP to bind.
            port: TCP port to bind.
            poll_timeout: Timeout in milliseconds for poll() to check for requests.
        """
        self._metadata = {}
        self._port = port

        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False

        self._context = context
        self._socket = self._context.socket(zmq.REP)
        self._socket.bind(f"tcp://*:{self._port}")
        self._running = True

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while self._running:
            try:
                socks = dict(poller.poll(timeout=200))
                if self._socket in socks and socks[self._socket] == zmq.POLLIN:
                    _ = self._socket.recv()  # receive request
                    self._socket.send_json(self._metadata)
            except zmq.ZMQError:
                if not self._running:
                    break  # normal exit when stopping
            except Exception as e:
                print(f"Unexpected error in Responser: {e}")

    # --------------------------------------------------------
    # public api
    # --------------------------------------------------------
    def register(self, name, metadata):
        if name in self._metadata:
            raise ValueError(f"Metadata with name '{name}' is already registered.")
        self._metadata.update({name: metadata})

    def get_port(self):
        return self._port

    def get_metadata(self):
        return self._metadata

    def close(self):
        """Stop the Responser thread and close ZMQ resources."""
        self._running = False
        self._thread.join(timeout=1)
        if self._thread.is_alive():
            print("Responser thread did not stop gracefully")
        try:
            self._socket.close()
            if self._use_temp_context:
                self._context.term()
        except Exception as e:
            print(f"Error closing Responser socket: {e}")


def request_metadata(
    context: Optional[zmq.Context] = None, server_ip: str = "0.0.0.0", port: int = 60001
) -> Optional[Dict[str, Any]]:
    """ZMQ REQ function to request metadata from server."""
    metadata = None
    try:
        _use_temp_context = context is None
        if _use_temp_context:
            context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)  # do not wait on close
        socket.connect(f"tcp://{server_ip}:{port}")

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        msg = b"GET_DATA"
        socket.send(msg)
        socks = dict(poller.poll(timeout=1000))

        if socket in socks and socks[socket] == zmq.POLLIN:
            metadata = socket.recv_json()
            if metadata is not None:
                print(f"Received metadata from {server_ip}:{port}")
        else:
            print(f"Request to {server_ip}:{port} timed out or no response.")

        socket.close()
        if _use_temp_context:
            context.term()

        return metadata
    except Exception as e:
        print(f"Unexpected error in Requester: {e}")
        return metadata
