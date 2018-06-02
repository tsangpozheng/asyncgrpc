import asyncio
import collections
import logging
import struct
from typing import List, Tuple

import grpc
from grpc._cython import cygrpc
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.errors import ErrorCodes
from h2.events import (ConnectionTerminated, DataReceived, RequestReceived,
                       ResponseReceived, TrailersReceived, StreamEnded,
                       StreamReset)
from h2.exceptions import ProtocolError

CONTENT_TYPES = {'application/grpc', 'application/grpc+proto'}


def _handle_call(stream, method_finder):
    h2_method = stream.headers[':method']
    if h2_method != 'POST':
        stream.send_headers([(':status', '405')], end_stream=True)
        return None

    h2_content_type = stream.headers['content-type']
    if h2_content_type not in CONTENT_TYPES:
        stream.send_trailers_content_type_missing()
        return None

    h2_path = stream.headers[':path']
    method_handler = method_finder(h2_path)
    if method_handler is None:
        stream.send_trailers_unimplemented()
        return None

    return asyncio.create_task(
        _handle_with_method_handler(stream, method_handler))


async def _handle_with_method_handler(stream, method_handler):
    try:
        stream.send_response_headers()

        if method_handler.request_streaming:
            if method_handler.response_streaming:
                await _handle_stream_stream(stream, method_handler)
            else:
                await _handle_stream_unary(stream, method_handler)
        else:
            if method_handler.response_streaming:
                await _handle_unary_stream(stream, method_handler)
            else:
                await _handle_unary_unary(stream, method_handler)

        stream.send_response_trailers()
    except Exception as e:
        logging.exception(e)
        logging.error('stream task error info:', stream.stream_id,
                      stream.headers)
        stream.send_response_trailers(message='error')


async def _handle_unary_unary(stream, method_handler):
    req_message = await stream.get_message_unary(
        method_handler.request_deserializer)
    res_message = await method_handler.unary_unary(req_message)
    stream.send_message_data(res_message, method_handler.response_serializer)


async def _handle_stream_unary(stream, method_handler):
    req_message = stream.get_message_stream(
        method_handler.request_deserializer)
    res_message = await method_handler.stream_unary(req_message)
    stream.send_message_data(res_message, method_handler.response_serializer)


async def _handle_unary_stream(stream, method_handler):
    req_message = await stream.get_message_unary(
        method_handler.request_deserializer)
    async for res_message in method_handler.unary_stream(req_message):
        stream.send_message_data(res_message,
                                 method_handler.response_serializer)


async def _handle_stream_stream(stream, method_handler):
    req_message = stream.get_message_stream(
        method_handler.request_deserializer)
    async for res_message in method_handler.stream_stream(req_message):
        stream.send_message_data(res_message,
                                 method_handler.response_serializer)


class GrpcStream(object):
    def __init__(self,
                 transport,
                 h2conn,
                 stream_id,
                 call_meta=None,
                 method_finder=None,
                 headers=None):
        self.transport = transport
        self.conn = h2conn
        self.stream_id = stream_id
        self.method_finder = method_finder
        self.headers = headers
        self.reader = asyncio.StreamReader()
        self.response_headers = asyncio.Queue()
        self.response_trailers = asyncio.Queue()

    async def read_message_data(self):
        try:
            meta = await self.reader.readexactly(5)
        except:
            return None
        compressed_flag = struct.unpack('?', meta[:1])[0]
        if compressed_flag:
            raise NotImplementedError('Compression not implemented')

        message_len = struct.unpack('>I', meta[1:])[0]
        message_bin = await self.reader.readexactly(message_len)
        return message_bin

    async def get_message_stream(self, deserializer):
        while True:
            data = await self.read_message_data()
            if data is None:
                return
            message = deserializer(data)
            yield message

    async def get_message_unary(self, deserializer):
        data = await self.read_message_data()
        if data is None:
            return None
        message = deserializer(data)
        return message

    def send_data(self, data: bytes = None, end_stream=False):
        self.conn.send_data(self.stream_id, data, end_stream=end_stream)
        self.transport.write(self.conn.data_to_send())
        logging.debug('local_flow_control_window: %s',
                      self.conn.local_flow_control_window(self.stream_id))

    def send_header(self, headers: List[Tuple], end_stream=False):
        self.conn.send_headers(self.stream_id, headers, end_stream=end_stream)
        self.transport.write(self.conn.data_to_send())

    def end_stream(self):
        self.conn.end_stream(self.stream_id)
        self.transport.write(self.conn.data_to_send())

    def send_message_data(self, message, serializer, end_stream=False):
        message_bin = serializer(message)
        data = (struct.pack('?', False) + struct.pack('>I', len(message_bin)) +
                message_bin)
        self.send_data(data, end_stream=end_stream)

    def send_request_headers(self, authority, rpc_method):
        self.send_header(
            [(':method', 'POST'), (':scheme', 'http'), (':path', rpc_method),
             (':authority', authority), ('te', 'trailers'),
             ('content-type', 'application/grpc+proto'),
             ('user-agent', 'grpc-python-asyncio')],
            end_stream=False)

    def send_response_headers(self):
        self.send_header(
            [(':status', '200'), ('content-type', 'application/grpc+proto')],
            end_stream=False)

    def send_response_trailers(self, status=cygrpc.StatusCode.ok,
                               message=None):
        if message:
            self.send_header(
                [('grpc-status', str(status)), ('grpc-message', message)],
                end_stream=True)
        else:
            self.send_header([('grpc-status', str(status))], end_stream=True)

    def send_trailers_content_type_missing(self):
        self.send_header(
            [(':status', '415'),
             ('grpc-status', str(cygrpc.StatusCode.unknown)),
             ('grpc-message', 'Missing content-type header')],
            end_stream=True)

    def send_trailers_content_type_unknow(self):
        self.send_header(
            [(':status', '415'),
             ('grpc-status', str(cygrpc.StatusCode.unknown)),
             ('grpc-message', 'Unacceptable content-type header')],
            end_stream=True)

    def send_trailers_unknown(self):
        self.send_header(
            [(':status', '200'),
             ('grpc-status', str(cygrpc.StatusCode.unknown)),
             ('grpc-message', 'Error in service handler!')],
            end_stream=True)

    def send_trailers_unimplemented(self):
        self.send_header(
            [(':status', '200'),
             ('grpc-status', str(cygrpc.StatusCode.unimplemented)),
             ('grpc-message', 'Method not found!')],
            end_stream=True)

    def send_trailers_resource_exhausted(self):
        self.send_header(
            [(':status', '200'),
             ('grpc-status', str(cygrpc.StatusCode.resource_exhausted)),
             ('grpc-message', 'Concurrent RPC limit exceeded!')],
            end_stream=True)

    # call by protocal

    def start_rpc_task(self):
        self.task = _handle_call(self, self.method_finder)

    def invoke_rpc_call(self, authority, call_meta, call_request):
        self.task = _invoke_call(self, authority, call_meta, call_request)
        return self.task

    def cancel_rpc_task(self):
        if self.task:
            self.task.cancel()
            self.task = None


class GrpcProtocol(asyncio.Protocol):
    def __init__(self, client_side=True, method_finder=None):
        print('client side', client_side)
        self.conn = H2Connection(
            config=H2Configuration(
                client_side=client_side, header_encoding='utf-8'))
        self.transport = None
        self.method_finder = method_finder

    def connection_made(self, transport):
        self.conn.initiate_connection()
        self.transport = transport
        self.transport.write(self.conn.data_to_send())
        self.streams = {}
        logging.info('h2 connection_mades %s', transport)

    def connection_lost(self, exc):
        logging.info('h2 connection_lost %s', exc)
        for stream in self.streams.values():
            stream.cancel_rpc_task()
        self.streams = None

    def data_received(self, data):
        try:
            events = self.conn.receive_data(data)
        except ProtocolError as e:
            self.transport.write(self.conn.data_to_send())
            self.transport.close()
        else:
            self.transport.write(self.conn.data_to_send())
            for event in events:
                logging.info('h2.event: %s', event)
                if isinstance(event, RequestReceived):
                    self.request_received(event.stream_id, event.headers)
                elif isinstance(event, ResponseReceived):
                    self.response_received(event.stream_id, event.headers)
                elif isinstance(event, TrailersReceived):
                    self.trailers_received(event.stream_id, event.headers)
                elif isinstance(event, DataReceived):
                    self.stream_data_received(event.stream_id, event.data)
                elif isinstance(event, StreamEnded):
                    self.stream_ended(event.stream_id)
                elif isinstance(event, StreamReset):
                    self.stream_reset(event.stream_id)
                elif isinstance(event, ConnectionTerminated):
                    self.transport.close()
                else:
                    logging.debug('ignore h2 event: %s', event)

                self.transport.write(self.conn.data_to_send())

    # h2 protocol events

    def request_received(self, stream_id: int, headers: List[Tuple[str, str]]):
        headers_dict = collections.OrderedDict(headers)
        stream = GrpcStream(
            self.transport,
            self.conn,
            stream_id,
            method_finder=self.method_finder,
            headers=headers_dict)
        stream.start_rpc_task()
        self.streams[stream_id] = stream

    def response_received(self, stream_id: int,
                          headers: List[Tuple[str, str]]):
        headers_dict = collections.OrderedDict(headers)
        self.streams[stream_id].response_headers.put_nowait(headers_dict)
        logging.debug('response_received: %s, %s', stream_id, headers)
        logging.debug('protocol.streams: %s', self.streams)

    def trailers_received(self, stream_id: int,
                          headers: List[Tuple[str, str]]):
        headers_dict = collections.OrderedDict(headers)
        self.streams[stream_id].response_trailers.put_nowait(headers_dict)

    def stream_data_received(self, stream_id: int, data: bytes):
        try:
            stream = self.streams[stream_id]
        except KeyError:
            self.conn.reset_stream(
                stream_id, error_code=ErrorCodes.PROTOCOL_ERROR)
        else:
            stream.reader.feed_data(data)
            logging.debug('remote_flow_control_window: %s',
                          self.conn.remote_flow_control_window(stream_id))
            if self.conn.remote_flow_control_window(stream_id) < 1024:
                self.conn.increment_flow_control_window(4194304)
                self.conn.increment_flow_control_window(41943040, stream_id)
                self.transport.write(self.conn.data_to_send())

    def stream_ended(self, stream_id: int):
        logging.debug('stream complete %s', stream_id)
        try:
            stream = self.streams[stream_id]
        except KeyError:
            self.conn.reset_stream(
                stream_id, error_code=ErrorCodes.PROTOCOL_ERROR)
        else:
            stream.reader.feed_eof()
            del self.streams[stream_id]

    def stream_reset(self, stream_id: int):
        logging.info('stream reset %s', stream_id)
        stream = self.streams.get(stream_id)
        if stream:
            stream.cancel_rpc_task()
            del self.streams[stream_id]

    def invoke_rpc_call(self, authority, call_meta, call_request):
        stream_id = self.conn.get_next_available_stream_id()
        stream = GrpcStream(self.transport, self.conn, stream_id)
        self.streams[stream_id] = stream
        logging.debug('protocol.invoke_rpc_call %s, %s', stream_id, call_meta)
        return stream.invoke_rpc_call(authority, call_meta, call_request)

    def close(self):
        self.conn.close_connection()
        self.transport.write(self.conn.data_to_send())


def _send_request_stream(stream, call_meta, call_request):
    if isinstance(call_request, collections.AsyncIterator):
        return asyncio.create_task(
            _send_request_stream_async(stream, call_meta, call_request))
    elif isinstance(call_request, collections.Iterator):
        for message in call_request:
            stream.send_message_data(message, call_meta.request_serializer)
        stream.end_stream()
        return None
    else:
        raise Exception('request is not a Iterator')


async def _send_request_stream_async(stream, call_meta, call_request):
    async for message in call_request:
        stream.send_message_data(message, call_meta.request_serializer)
    stream.end_stream()


async def _read_response_trailers(stream):
    trailers = await stream.response_trailers.get()
    if trailers['grpc-status'] != str(cygrpc.StatusCode.ok):
        raise grpc.RpcError('grpc-status return not ok',
                            trailers.get('grpc-message'))


def _invoke_call(stream, authority, call_meta, call_request):
    stream.send_request_headers(authority, call_meta.method)
    return asyncio.create_task(
        _invoke_with_request(stream, authority, call_meta, call_request))


async def _invoke_with_request(stream, authority, call_meta, call_request):
    headers = await stream.response_headers.get()
    logging.info('stream.response_headers.get(): %s, %s', stream.stream_id,
                 headers)

    if headers[':status'] != '200':
        raise grpc.RpcError(':status return non 200')

    logging.info('_invoke_with_request %s', call_meta)

    if call_meta.request_streaming:
        if call_meta.response_streaming:
            return _invoke_stream_stream(stream, call_meta, call_request)
        else:
            return await _invoke_stream_unary(stream, call_meta, call_request)
    else:
        if call_meta.response_streaming:
            return _invoke_unary_stream(stream, call_meta, call_request)
        else:
            return await _invoke_unary_unary(stream, call_meta, call_request)


async def _invoke_stream_stream(stream, call_meta, call_request):
    reqtask = _send_request_stream(stream, call_meta, call_request)
    try:
        while True:
            response = await stream.get_message_unary(
                call_meta.response_deserializer)
            if response is None:
                break
            yield response
    except:
        raise
    else:
        await _read_response_trailers(stream)
    finally:
        if reqtask:
            reqtask.cancel()


async def _invoke_stream_unary(stream, call_meta, call_request):
    reqtask = _send_request_stream(stream, call_meta, call_request)
    response = await stream.get_message_unary(call_meta.response_deserializer)
    if reqtask:
        reqtask.cancel()
    await _read_response_trailers(stream)
    return response


async def _invoke_unary_stream(stream, call_meta, call_request):
    stream.send_message_data(
        call_request, call_meta.request_serializer, end_stream=True)
    while True:
        response = await stream.get_message_unary(
            call_meta.response_deserializer)
        if response is None:
            break
        yield response
    await _read_response_trailers(stream)


async def _invoke_unary_unary(stream, call_meta, call_request):
    stream.send_message_data(
        call_request, call_meta.request_serializer, end_stream=True)
    response = await stream.get_message_unary(call_meta.response_deserializer)
    await _read_response_trailers(stream)
    return response
