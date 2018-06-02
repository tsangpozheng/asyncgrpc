import sys
sys.path.append('.')
sys.path.append('../pybase/')

import logging
import asyncio
from aiogrpc import Channel

from protos import benchmark_pb2
from protos import benchmark_pb2_grpc

messages = [
    benchmark_pb2.HelloRequest(name='First'),
    benchmark_pb2.HelloRequest(name='Second'),
    benchmark_pb2.HelloRequest(name='3'),
    benchmark_pb2.HelloRequest(name='4'),
]


def generate_messages():
    for msg in messages:
        print("Sending %s" % (msg.name))
        yield msg


async def generate_messages_async():
    for msg in messages:
        await asyncio.sleep(1)
        print("Sending async %s" % (msg.name))
        yield msg


async def test(stub):
    responses = await stub.SayHelloSS(generate_messages())
    async for response in responses:
        print("Received SS %s" % (response.message))

    responses = await stub.SayHelloSS(generate_messages_async())
    async for response in responses:
        print("Received async SS %s" % (response.message))

    response = await stub.SayHelloSU(generate_messages())
    print("Received SU %s" % (response.message))

    response = await stub.SayHelloSU(generate_messages_async())
    print("Received async SU %s" % (response.message))

    responses = await stub.SayHelloUS(benchmark_pb2.HelloRequest(name='im US'))
    async for response in responses:
        print("Received US %s" % (response.message))

    response = await stub.SayHelloUU(benchmark_pb2.HelloRequest(name='im UU'))
    print("Received UU %s" % (response.message))


async def test0(stub):

    responses = await stub.SayHelloSS(generate_messages())
    async for response in responses:
        print("Received SS %s" % (response.message))

    responses = await stub.SayHelloSS(generate_messages_async())
    async for response in responses:
        print("Received SS %s" % (response.message))


async def atest(stub):
    await test(stub)
    await test(stub)


logging.basicConfig(level=logging.INFO)
stub = benchmark_pb2_grpc.GreeterStub(Channel('127.0.0.1:5001'))


async def bench():
    for i in range(2000):
        responses = await stub.SayHelloSS(generate_messages())
        async for response in responses:
            print("Received SS %s" % (response.message))

        response = await stub.SayHelloSU(generate_messages())
        print("Received SU %s" % (response.message))

        responses = await stub.SayHelloUS(
            benchmark_pb2.HelloRequest(name='im US'))
        async for response in responses:
            print("Received US %s" % (response.message))

        response = await stub.SayHelloUU(
            benchmark_pb2.HelloRequest(name='im UU'))
        print("Received UU %s" % (response.message))


def run():
    loop = asyncio.get_event_loop()
    t1 = asyncio.ensure_future(test(stub))
    t2 = asyncio.ensure_future(test(stub))
    loop.run_until_complete(asyncio.wait([t1, t2]))


def run():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bench())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()
