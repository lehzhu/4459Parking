# 4459Parking

## Distributed Parking System with Real-Time Video Processing

This project simulates a distributed system for tracking parking availability on a university campus using camera feeds. It implements the RAFT consensus algorithm to ensure the processing servers remain available even during server failures.

## System Overview

- **Camera Servers**: Output a frame of video every 10 seconds, each containing a random string
- **Processing Servers**: Calculate parking spaces from camera frames using a hash function
- **RAFT Consensus**: Ensures the processing system remains operational even if servers fail
- **Fault Tolerance**: The system handles both camera failures and processing server failures gracefully

## 📋 Components

### Camera System
- `camera_video_client.py`: Simulates cameras sending frames, with unique camera IDs
- `video_stream.proto`: Definitions for the video streaming service

### Processing System
- `raft_video_processor.py`: Processes video frames with RAFT consensus to calculate parking spaces
- `raft.proto`: Protocol buffer definitions for RAFT RPCs

### Testing & Utilities
- `start_raft_cluster.py`: Helper script to start a cluster of RAFT nodes
- `test_parking_system.py`: Testing script to verify system stability and fault tolerance

## 🔧 Setup Instructions

### Prerequisites
- Python 3.6 or higher
- pip package manager

### Installation

1. Clone the repository:
```
git clone https://github.com/lehzhu/4459Parking.git
cd 4459Parking
```

2. Install required packages:
```
pip install -r requirements.txt
```

3. Generate gRPC code from proto files:
```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. *.proto
```
   Or simply run:
```
make
```

## 🚀 Running the System

### Starting RAFT Processing Servers

Use the helper script to start a cluster of 3 RAFT nodes (recommended):
```
python start_raft_cluster.py
```

Or start individual nodes manually:
```
python raft_video_processor.py --id 1 --port 60061 --peers "2:localhost:60062,3:localhost:60063"
python raft_video_processor.py --id 2 --port 60062 --peers "1:localhost:60061,3:localhost:60063"
python raft_video_processor.py --id 3 --port 60063 --peers "1:localhost:60061,2:localhost:60062"
```

### Starting Camera Clients

Start one or more camera clients (in separate terminals):
```
python camera_video_client.py --camera-id 1 --frame-interval 10
python camera_video_client.py --camera-id 2 --frame-interval 10
```

Each camera needs a unique ID. The `--frame-interval` parameter controls how often (in seconds) the camera sends a frame.

## 🧪 Testing the System

The system includes a test script that verifies stability and fault tolerance. You can run this script by itself, but only after installing packages and 
generating proto files

```
python test_parking_system.py
```

This will run a series of tests:
1. **Test #1**: System Stability & Activity Monitoring
2. **Test #2**: RAFT Node Failure Tolerance
3. **Test #3**: Camera Failure Tolerance 
4. **Test #4**: RAFT Node Recovery
5. **Test #5**: Camera Recovery

Test logs are stored in the `test_logs` directory.

## 🔍 How It Works

1. Camera clients generate random "frames" and send them to processing servers
2. RAFT nodes elect a leader to handle processing
3. The leader calculates parking spaces from each frame using a hash function
4. Results are replicated to follower nodes for fault tolerance
5. If a leader fails, a new one is automatically elected
6. Cameras automatically discover and connect to the current leader

## 🐞 Troubleshooting

- **Connection Refused**: Ensure all RAFT nodes are started and have had enough time to elect a leader
- **Leader Election Issues**: Check logs in `test_logs` directory for timing problems
- **Camera Connection Issues**: Verify the ports match between camera clients and processing servers

## 🔄 RAFT Consensus Algorithm

The implementation follows the protocol described in the [RAFT paper](https://raft.github.io/raft.pdf) with adaptations for video processing.

Key features:
- Leader election for determining which node processes requests
- Log replication for ensuring data consistency across nodes
- Automatic failure detection and recovery
- Camera client server discovery to find the current leader
