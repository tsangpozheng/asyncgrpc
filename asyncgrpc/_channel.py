import grpc
import asyncio
import logging
import collections
import functools

from ._protocol import GrpcProtocol

CallMeta = collections.namedtuple('CallMeta', (
    'method',
    'request_streaming',
    'response_streaming',
    'request_serializer',
    'response_deserializer',
))


def parse_address(address):
    hp = address.rsplit(':', 1)
    return hp[0], int(hp[1])


class Channel(grpc.Channel):
    """A asyncio-backed implementation of grpc.Channel."""

    def __init__(self, target):
        self.authority = target
        self.target = (parse_address(target), None)
        self.protocol = None
        self.lock = asyncio.Lock()

    def close(self):
        if self.protocol:
            self.protocol.close()

    def protocol_factory(self):
        return GrpcProtocol(client_side=True)

    def _multi_callable(self, meta):
        return functools.partial(self._invoke_with_request, meta)

    async def _invoke_with_request(self, meta, request):
        with (await self.lock):
            if self.protocol is None:
                loop = asyncio.get_event_loop()
                _, protocol = await loop.create_connection(
                    self.protocol_factory, self.target[0][0],
                    self.target[0][1])
                self.protocol = protocol
        logging.debug('leave lock')
        return await self.protocol.invoke_rpc_call(self.authority, meta,
                                                   request)

    def subscribe(self, callback, try_to_connect=None):
        raise NotImplementedError()

    def unsubscribe(self, callback):
        raise NotImplementedError()

    def unary_unary(self,
                    method,
                    request_serializer=None,
                    response_deserializer=None):
        call_meta = CallMeta(method, False, False, request_serializer,
                             response_deserializer)
        return self._multi_callable(call_meta)

    def unary_stream(self,
                     method,
                     request_serializer=None,
                     response_deserializer=None):
        call_meta = CallMeta(method, False, True, request_serializer,
                             response_deserializer)
        return self._multi_callable(call_meta)

    def stream_unary(self,
                     method,
                     request_serializer=None,
                     response_deserializer=None):
        call_meta = CallMeta(method, True, False, request_serializer,
                             response_deserializer)
        return self._multi_callable(call_meta)

    def stream_stream(self,
                      method,
                      request_serializer=None,
                      response_deserializer=None):
        call_meta = CallMeta(method, True, True, request_serializer,
                             response_deserializer)
        return self._multi_callable(call_meta)
