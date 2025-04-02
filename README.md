# 4459Parking

## Distributed Parking System with Real-Time Video Streaming

This project simulates a distributed system for tracking parking availability on a university campus. It includes both traditional update replication (primary–backup model) and a simulated real-time video stream from campus cameras. RAFT is implemented to handle server outages. 

## 🧱 Components

### 🚗 Parking System
- parking_primary.py: Handles write requests from clients and forwards updates to the backup.
- parking_backup.py: Stores replicated parking updates.
- parking.proto: gRPC definitions for parking update messages and services.

### 🎥 Video Streaming System
- camera_video_client.py: Simulates a camera sending a live video feed (frame-by-frame).
- video_processor.py: Receives video frames and processes them (e.g. to detect open spots).
- video_stream.proto: gRPC definitions for video streaming messages and services.

### 🔄 RAFT Consensus for Fault Tolerance
- raft_video_processor.py: Video processor with RAFT consensus implementation.
- raft.proto: Protocol buffer definitions for RAFT RPCs.
- camera_video_client.py: Enhanced to discover and reconnect to RAFT leaders.

## Required Libraries/Packages

Make sure you have Python and PIP installed before proceeding. 

Run the following command to download required python packages:
```
pip install grpcio-tools protobuf 
```

## 🛠 How to Run:

1. Generate gRPC files from .proto files:
```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. parking.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. video_stream.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. heartbeat_service.proto
```

2. Start the parking services (in separate terminals):
```
python heartbeat_service.py
python parking_backup.py
python parking_primary.py
python video_processor_with_heartbeat.py
python camera_video_client_with_heartbeat.py
```
Monitor heartbeat service health:
```
python heartbeat_check.py --monitor
```

3. To use traditional video processing service:
```
python video_processor.py
```

4. Or to use RAFT-based fault-tolerant video processing (in separate terminals):
Start 3 RAFT nodes:
```
python raft_video_processor.py --id 1 --port 60061 --peers "2:localhost:60062,3:localhost:60063"
python raft_video_processor.py --id 2 --port 60062 --peers "1:localhost:60061,3:localhost:60063"
python raft_video_processor.py --id 3 --port 60063 --peers "1:localhost:60061,2:localhost:60063"
```

5. Run the camera simulation:
```
python camera_video_client.py
```

## 📦 What It Simulates
- A campus camera sends live "video frames" to a central processing node.
- The processor extracts simulated data from each frame (e.g., parking availability).
- Parking data is replicated to a backup server for fault tolerance.
- With RAFT, video processing can continue even if nodes fail.

## ✅ Features
- Distributed primary–backup replication for parking data.
- Real-time gRPC streaming of video frames.
- RAFT consensus algorithm for fault-tolerant video processing.
- Automatic leader election and log replication with RAFT.
- Client-side server discovery to find the current leader.
- Easy to extend with actual computer vision or ML processing.

## 🧠 RAFT Consensus Algorithm

The RAFT consensus algorithm implementation enables fault tolerance for video processing by:
- Electing a leader to handle client requests
- Replicating video processing logs to follower nodes
- Maintaining consistency when nodes fail
- Automatically handling leader failures through re-election
- Forwarding client requests to the current leader

The implementation follows the protocol described in the [RAFT paper](https://raft.github.io/raft.pdf) with adaptations for video processing.
