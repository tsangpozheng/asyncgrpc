import asyncio

import grpc
from grpc import _interceptor
from grpc._server import _HandlerCallDetails

from ._protocol import GrpcProtocol


def parse_address(address):
    hp = address.rsplit(':', 1)
    return hp[0], int(hp[1])


class Server(grpc.Server):
    def __init__(self, interceptors=None):
        self.address = None
        self.generic_handlers = []
        self.interceptor_pipeline = None
        self.server = None
        if interceptors:
            self.interceptor_pipeline = _interceptor.service_pipeline(
                interceptors)

    def add_generic_rpc_handlers(self, generic_rpc_handlers):
        self.generic_handlers.extend(generic_rpc_handlers)

    def add_insecure_port(self, address):
        self.address = (parse_address(address), None)

    def add_secure_port(self, address, certfile, keyfile):
        import ssl
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.options |= (ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
                                | ssl.OP_NO_COMPRESSION)
        ssl_context.set_ciphers("ECDHE+AESGCM")
        ssl_context.load_cert_chain(certfile="cert.crt", keyfile="cert.key")
        ssl_context.set_alpn_protocols(["h2"])
        self.address = (parse_address(address), ssl_context)

    def start(self):
        loop = asyncio.get_event_loop()
        coro = loop.create_server(
            self.protocol_factory,
            self.address[0][0],
            self.address[0][1],
            ssl=self.address[1])
        server = loop.run_until_complete(coro)
        print('Serving on {}'.format(server.sockets[0].getsockname()))
        self.server = server

    def stop(self, grace):
        if self.server is None:
            return
        self.server.close()
        if grace:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.server.wait_closed())
        self.server = None

    def __del__(self):
        self.stop(None)

    def protocol_factory(self):
        return GrpcProtocol(
            client_side=False, method_finder=self.find_method_handler)

    def find_method_handler(self, rpc_method):
        def query_handlers(handler_call_details):
            for generic_handler in self.generic_handlers:
                method_handler = generic_handler.service(handler_call_details)
                if method_handler is not None:
                    return method_handler
            return None

        handler_call_details = _HandlerCallDetails(rpc_method, None)

        if self.interceptor_pipeline is not None:
            return self.interceptor_pipeline.execute(query_handlers,
                                                     handler_call_details)
        else:
            return query_handlers(handler_call_details)
