compile:
	python3 -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. --pyi_out=. parking.proto video_stream.proto heartbeat_service.proto
	$(MAKE) -C raft
