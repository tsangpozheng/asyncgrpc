# asyncgrpc Package

This is a Python 3.6 module package implementing the grpc protocol
both server side and client side.

This package uses the 'async/await [for]' syntax from Python 3.6,
uses the asyncio module from Python 3.

the server use the default loop (from asyncio.get_event_loop) to run
the handler coro.

grpc timeout has not been implemented.
