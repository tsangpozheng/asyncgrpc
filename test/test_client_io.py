import sys
sys.path.append('.')

import grpc

from protos import benchmark_pb2
from protos import benchmark_pb2_grpc


def generate_messages():
    messages = [
        benchmark_pb2.HelloRequest(name='First'),
        benchmark_pb2.HelloRequest(name='Second'),
        benchmark_pb2.HelloRequest(name='3'),
        benchmark_pb2.HelloRequest(name='4'),
    ]
    for msg in messages:
        print("Sending %s" % (msg.name))
        yield msg


channel = grpc.insecure_channel('127.0.0.1:5001')
stub = benchmark_pb2_grpc.GreeterStub(channel)


def run():
    responses = stub.SayHelloSS(generate_messages())
    for response in responses:
        print("Received SS %s" % (response.message))

    response = stub.SayHelloSU(generate_messages())
    print("Received SU %s" % (response.message))

    responses = stub.SayHelloUS(benchmark_pb2.HelloRequest(name='im US'))
    for response in responses:
        print("Received US %s" % (response.message))

    response = stub.SayHelloUU(benchmark_pb2.HelloRequest(name='im UU'))
    print("Received UU %s" % (response.message))


if __name__ == '__main__':
    for i in range(1):
        run()
