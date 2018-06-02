

gen:
	python3 -m grpc_tools.protoc -I./protos/ --python_out=./protos/ --grpc_python_out=./protos/ ./protos/*.proto
	protoc --go_out=plugins=grpc:./protos -I=./protos ./protos/*.proto

client_go:
	go run go_client/main.go

server_go:
	go run go_server/main.go

server_pyio:
	python test_server_io.py

server_pyaio:
	python test_server_aio.py
