"""Event loop implementation that uses the `asyncio` standard module.

The `asyncio` module was added to python standard library on 3.4, and it
provides a pure python implementation of an event loop library. It is used
as a fallback in case pyuv is not available(on python implementations other
than CPython).

Earlier python versions are supported through the `trollius` package, which
is a backport of `asyncio` that works on Python 2.6+.
"""
from __future__ import absolute_import

import logging
import os
import sys
from collections import deque
import threading
from msgpack import Unpacker
try:
    # For python 3.4+, use the standard library module
    import asyncio
except (ImportError, SyntaxError):
    # Fallback to trollius
    import trollius as asyncio

from .base import BaseEventLoop

logger = logging.getLogger(__name__)
debug, info, warn = (logger.debug, logger.info, logger.warning,)

loop_cls = asyncio.SelectorEventLoop
if os.name == 'nt':
    from asyncio.windows_utils import PipeHandle

    # On windows use ProactorEventLoop which support pipes and is backed by the
    # more powerful IOCP facility
    # NOTE: we override in the stdio case, because it doesn't work.
    loop_cls = asyncio.ProactorEventLoop

    import msvcrt
    from ctypes import (cast, cdll, byref, c_int, c_long, c_size_t, c_ulong,
                        c_void_p, WinError, POINTER, Structure, Union, sizeof)
    from ctypes.wintypes import HANDLE, DWORD

    LPDWORD = POINTER(DWORD)

    STATUS_SUCCESS = 0x00000000
    FileModeInformation = c_int(16)
    FILE_SYNCHRONOUS_IO_ALERT = 0x00000010
    FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020

    class IO_STATUS_BLOCK(Union):
        _fields_ = [("Status", c_long), ("Pointer", c_void_p)]

    class FILE_MODE_INFORMATION(Structure):
        pass

    FILE_MODE_INFORMATION._fields_ = [
        ("Mode", c_ulong),
        ("FFILE_MODE_INFORMATION)", POINTER(FILE_MODE_INFORMATION))]

    def is_overlapped_pipe(handle):
        try:
            ntdll = cdll.LoadLibrary("ntdll.dll")
            NtQueryInformationFile = ntdll.NtQueryInformationFile
            NtQueryInformationFile.argtypes = [HANDLE,
                                               POINTER(IO_STATUS_BLOCK),
                                               c_void_p,
                                               c_size_t,
                                               c_int]
            NtQueryInformationFile.restype = c_ulong
        except OSError as e:
            debug("%s", e)
            return False

        io_status = IO_STATUS_BLOCK()
        mode_info = FILE_MODE_INFORMATION()
        res = NtQueryInformationFile(handle,
                                     byref(io_status),
                                     cast(byref(mode_info), c_void_p),
                                     sizeof(mode_info),
                                     FileModeInformation)
        if res != STATUS_SUCCESS:
            debug(WinError())
            return False

        if mode_info.Mode & FILE_SYNCHRONOUS_IO_ALERT \
                or mode_info.Mode & FILE_SYNCHRONOUS_IO_NONALERT:
            return False
        return True


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
        self._raw_stdout = False
        self._active = False

    def _connect_tcp(self, address, port):
        coroutine = self._loop.create_connection(self._fact, address, port)
        self._loop.run_until_complete(coroutine)

    def _connect_socket(self, path):
        if os.name == 'nt':
            coroutine = self._loop.create_pipe_connection(self._fact, path)
        else:
            coroutine = self._loop.create_unix_connection(self._fact, path)
        self._loop.run_until_complete(coroutine)

    def _stdin_reader(self):
        debug("started reader thread")
        unpacker = Unpacker()
        stored_data = b''
        while True:  # self._active
            try:
                data = self._stdin.read(1)
                stored_data += data
                unpacker.feed(data)
                next(unpacker)
                debug("reader thread read %d", len(stored_data))
                self._loop.call_soon_threadsafe(self.data_received,
                                                stored_data)
                stored_data = b''
            except StopIteration:
                pass
            except Exception:
                debug("%s", sys.exc_info())

    def _connect_stdio(self):
        if os.name == 'nt':
            handle = msvcrt.get_osfhandle(sys.stdin.fileno())

        if os.name == 'nt' and not is_overlapped_pipe(handle):
            debug("none overlapped pipe, using reader thread")
            self._stdin = sys.stdin.buffer
            self._active = True
            threading.Thread(target=self._stdin_reader).start()
        else:
            if os.name == 'nt':
                debug("overlapped pipe, using native stdin connection")
                pipe = PipeHandle(handle)
            else:
                pipe = sys.stdin
            coroutine = self._loop.connect_read_pipe(self._fact, pipe)
            self._loop.run_until_complete(coroutine)

        try:
            if os.name == 'nt':
                pipe = PipeHandle(msvcrt.get_osfhandle(sys.stdout.fileno()))
            else:
                pipe = sys.stdout
            coroutine = self._loop.connect_write_pipe(self._fact, pipe)
            self._loop.run_until_complete(coroutine)
            debug("native stdout connection successful")
        except OSError:
            debug("native stdout connection failed, using blocking writes")

            self._stdout = sys.stdout.buffer
            self._raw_stdout = True

    def _connect_child(self, argv):
        if os.name != 'nt':
            self._child_watcher = asyncio.get_child_watcher()
            self._child_watcher.attach_loop(self._loop)
        coroutine = self._loop.subprocess_exec(self._fact, *argv)
        self._loop.run_until_complete(coroutine)

    def _start_reading(self):
        pass

    def _send(self, data):
        debug("sent %s %s", repr(data), str(self._raw_stdout))
        if self._raw_stdout:
            self._stdout.write(data)
            self._stdout.flush()
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
        # FIXME: this is racy and stuff
        self._active = False
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
