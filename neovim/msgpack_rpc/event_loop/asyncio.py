"""Event loop implementation that uses the `asyncio` standard module.

The `asyncio` module was added to python standard library on 3.4, and it
provides a pure python implementation of an event loop library. It is used
as a fallback in case pyuv is not available(on python implementations other
than CPython).

Earlier python versions are supported through the `trollius` package, which
is a backport of `asyncio` that works on Python 2.6+.
"""
from __future__ import absolute_import

import os
import sys
import logging
from collections import deque

logger = logging.getLogger(__name__)
debug, info, warn = (logger.debug, logger.info, logger.warning)

try:
    # For python 3.4+, use the standard library module
    import asyncio
except (ImportError, SyntaxError):
    # Fallback to trollius
    import trollius as asyncio

from .base import BaseEventLoop


loop_cls = asyncio.SelectorEventLoop
if os.name == 'nt':
    # On windows use ProactorEventLoop which support pipes and is backed by the
    # more powerful IOCP facility
    loop_cls = asyncio.ProactorEventLoop

    import msvcrt
    from ctypes import windll, byref, wintypes, WinError, POINTER
    from ctypes.wintypes import HANDLE, LPHANDLE, DWORD, BOOL

    LPDWORD = POINTER(DWORD)

    DUPLICATE_SAME_ACCESS = wintypes.DWORD(0x00000002)
    INVALID_HANDLE_VALUE = HANDLE(-1).value
    PIPE_READMODE_BYTE = wintypes.DWORD(0x00000000)
    PIPE_WAIT = wintypes.DWORD(0x00000000)

    import functools
    def _file_handle(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return msvcrt.get_osfhandle(func())
        return wrapper

    def dup_file(file, fmode='rb'):
        DuplicateHandle = windll.kernel32.DuplicateHandle
        DuplicateHandle.argtypes = [HANDLE,
                                    HANDLE,
                                    HANDLE,
                                    LPHANDLE,
                                    DWORD,
                                    BOOL,
                                    DWORD]
        DuplicateHandle.restype = BOOL

        SetNamedPipeHandleState = windll.kernel32.SetNamedPipeHandleState
        SetNamedPipeHandleState.argtypes = [HANDLE, LPDWORD, LPDWORD, LPDWORD]
        SetNamedPipeHandleState.restype = BOOL

        hsource = msvcrt.get_osfhandle(file.fileno())
        htarget = HANDLE()

        res = windll.kernel32.DuplicateHandle(INVALID_HANDLE_VALUE,
                                              hsource,
                                              INVALID_HANDLE_VALUE,
                                              byref(htarget),
                                              0,
                                              BOOL(False),
                                              DUPLICATE_SAME_ACCESS)
        if res == 0:
            print(WinError())
            return None

        mode = DWORD(0x00000000)
        res = windll.kernel32.SetNamedPipeHandleState(htarget,
                                                      byref(mode),
                                                      None,
                                                      None)
        if res == 0:
            print(WinError())
            return None

        fd = msvcrt.open_osfhandle(htarget.value, 0)
        f = os.fdopen(fd, mode=fmode)
        f.fileno = _file_handle(f.fileno)
        return f


class AsyncioEventLoop(BaseEventLoop, asyncio.Protocol,
                       asyncio.SubprocessProtocol):

    """`BaseEventLoop` subclass that uses `asyncio` as a backend."""

    def connection_made(self, transport):
        """Used to signal `asyncio.Protocol` of a successful connection."""
        self._transport = transport
        self._raw_transport = transport
        if isinstance(transport, asyncio.SubprocessTransport):
            self._transport = transport.get_pipe_transport(0)

    def connection_lost(self, exc):
        """Used to signal `asyncio.Protocol` of a lost connection."""
        self._on_error(exc.args[0] if exc else 'EOF')

    def data_received(self, data):
        """Used to signal `asyncio.Protocol` of incoming data."""
        if self._on_data:
            self._on_data(data)
            return
        self._queued_data.append(data)

    def pipe_connection_lost(self, fd, exc):
        """Used to signal `asyncio.SubprocessProtocol` of a lost connection."""
        self._on_error(exc.args[0] if exc else 'EOF')

    def pipe_data_received(self, fd, data):
        """Used to signal `asyncio.SubprocessProtocol` of incoming data."""
        if fd == 2:  # stderr fd number
            self._on_stderr(data)
        elif self._on_data:
            self._on_data(data)
        else:
            self._queued_data.append(data)

    def process_exited(self):
        """Used to signal `asyncio.SubprocessProtocol` when the child exits."""
        self._on_error('EOF')

    def _init(self):
        self._loop = loop_cls()
        self._queued_data = deque()
        self._fact = lambda: self
        self._raw_transport = None
        self._raw_stdio = False
        self._raw_stdout = None

    def _connect_tcp(self, address, port):
        coroutine = self._loop.create_connection(self._fact, address, port)
        self._loop.run_until_complete(coroutine)

    def _connect_socket(self, path):
        if os.name == 'nt':
            coroutine = self._loop.create_pipe_connection(self._fact, path)
        else:
            coroutine = self._loop.create_unix_connection(self._fact, path)
        self._loop.run_until_complete(coroutine)

    def _connect_stdio(self):
        if os.name == 'nt':
            coroutine = self._loop.connect_read_pipe(self._fact,
                                                     dup_file(sys.stdin))
            self._raw_stdio = True
            self._raw_stdout = sys.stdout
        else:
            coroutine = self._loop.connect_read_pipe(self._fact, sys.stdin)
        self._loop.run_until_complete(coroutine)
        if os.name != 'nt':
            coroutine = self._loop.connect_write_pipe(self._fact, sys.stdout)
            self._loop.run_until_complete(coroutine)

    def _connect_child(self, argv):
        if os.name != 'nt':
            self._child_watcher = asyncio.get_child_watcher()
            self._child_watcher.attach_loop(self._loop)
        coroutine = self._loop.subprocess_exec(self._fact, *argv)
        self._loop.run_until_complete(coroutine)

    def _start_reading(self):
        pass

    def _send(self, data):
        debug("sent %s %s", repr(data), str(self._raw_stdio))
        if self._raw_stdio:
            self._raw_stdout.buffer.write(data)
            self._raw_stdout.buffer.flush()
        else:
            self._transport.write(data)

    def _run(self):
        while self._queued_data:
            self._on_data(self._queued_data.popleft())
        self._loop.run_forever()

    def _stop(self):
        self._loop.stop()

    def _close(self):
        if self._raw_transport is not None:
            self._raw_transport.close()
        self._loop.close()

    def _threadsafe_call(self, fn):
        self._loop.call_soon_threadsafe(fn)

    def _setup_signals(self, signals):
        if os.name == 'nt':
            # add_signal_handler is not supported in win32
            self._signals = []
            return

        self._signals = list(signals)
        for signum in self._signals:
            self._loop.add_signal_handler(signum, self._on_signal, signum)

    def _teardown_signals(self):
        for signum in self._signals:
            self._loop.remove_signal_handler(signum)
