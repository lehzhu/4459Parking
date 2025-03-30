import dataclasses
from typing import Dict, Optional
from concurrent import futures

import sys
import os
# add parent dir to path to allow import of raft package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raft import GrpcTransport, PersistentState, RaftNode

import grpc

import add_pb2
import add_pb2_grpc
import raft.raft_pb2_grpc as raft_pb2_grpc


class ServerStateMachine:
    def __init__(self):
        self.sum = 0

    def apply(self, command: str) -> int:
        self.sum += int(command)
        return self.sum

    def reset(self) -> None:
        self.sum = 0


class AddService(add_pb2_grpc.AddServiceServicer):
    def __init__(self,  own_id: int, members: Dict[int, str], raft_node: RaftNode):
        self.id = own_id
        self.members = members
        self.raft_node = raft_node

    def Add(self, request: add_pb2.AddRequest, context) -> add_pb2.AddResponse:
        response = self.raft_node.append_log(str(request.value))
        if not response.success:
            return add_pb2.AddResponse(success=False, leader_address=self.members[response.leader], total=0)

        return add_pb2.AddResponse(success=True, leader_address="", total=response.result)


class RaftService(raft_pb2_grpc.RaftServicer):
    def __init__(self, raft_node: RaftNode):
        super().__init__()
        self.raft_node = raft_node

    def RequestVote(self, request, context):
        return self.raft_node.process_request_vote_request(request)

    def AppendEntries(self, request, context):
        return self.raft_node.process_append_entries_request(request)

@dataclasses.dataclass
class NodeInfo:
    transport: GrpcTransport
    fsm: ServerStateMachine
    add_service: AddService
    raft_service: RaftService
    server: Optional[grpc.Server]


def try_send(
    member_addresses: Dict[int, str], value: int
) -> Optional[int]:
    leader_address = None

    req = add_pb2.AddRequest(value=value)

    for i in member_addresses.keys():
        if leader_address is None:
            leader_address = member_addresses[i]

        channel = grpc.insecure_channel(leader_address)
        stub = add_pb2_grpc.AddServiceStub(channel)

        try:
            res = stub.Add(req)
        except Exception as ex:
            leader_address = None
            print(ex)
            continue

        if res.success:
            return res.total

        if res.leader_address:
            leader_address = res.leader_address

    return None


def process_user_commands(members: Dict[int, NodeInfo], member_addresses: Dict[int, str]) -> None:

    while True:
        user_input = input("Enter command:")
        if user_input == "add":
            user_input = input("Enter number:")
            try:
                int(user_input)
            except ValueError:
                print("That's not a valid number")
                continue

            res = try_send(member_addresses, int(user_input))
            if res is None:
                print("Failed to add input; server issue")
            else:
                print(f"Total is now {res}")

        elif user_input == "stop":
            return

        elif user_input == "stop node":
            user_input = input("Enter node id:")
            try:
                node_id = int(user_input)
            except ValueError:
                print("That's not a valid number")
                continue

            node_info = members[node_id]
            if node_info.server is None:
                print("Node not running")
                continue

            print(f"Stopping node with id {node_id}")
            node_info.add_service.raft_node.stop_raft_processing()
            node_info.server.stop(10)
            node_info.server.wait_for_termination()
            node_info.server = None
            print(f"Node stopped")

        elif user_input == "start node":
            user_input = input("Enter node id:")
            try:
                node_id = int(user_input)
            except ValueError:
                print("That's not a valid number")
                continue

            node_info = members[node_id]
            if node_info.server is not None:
                print("Node already running")
                continue

            print(f"Starting node with id {node_id}")
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            node_info.server = server
            add_pb2_grpc.add_AddServiceServicer_to_server(node_info.add_service, server)
            raft_pb2_grpc.add_RaftServicer_to_server(node_info.raft_service, server)
            node_info.server.start()
            node_info.add_service.raft_node.start_raft_processing()
            print(f"Node started")

        elif user_input == "leader id":
            for i in members:
                try:
                    print(f"node {i} says leader is {members[i].add_service.raft_node.get_leader_id()}")
                except Exception as ex:
                    print(ex)

        elif user_input == "fsm values":
            for i in members:
                print(f"node {i} fsm value is {members[i].fsm.sum}")

        elif user_input == "commit indices":
            for i in members:
                print(f"node {i} commit index is {members[i].add_service.raft_node._commit_index}")

        elif user_input == "timeout":
            user_input = input("Enter node id:")
            try:
                node_id = int(user_input)
            except ValueError:
                print("That's not a valid number")
                continue

            node_info = members[node_id]
            try:
                node_info.add_service.raft_node.force_timeout()
            except RuntimeError as ex:
                print(ex)


def main() -> None:
    member_ids = [1, 2, 3]
    member_addresses = {
        1: "[::]:50051",
        2: "[::]:50052", 
        3: "[::]:50053"
    }
    members: Dict[int, NodeInfo] = {}
    for id in member_ids:
        state = PersistentState(f"raft_state_{id}.raft")
        transport = GrpcTransport(id, member_addresses)
        transport.create_member_stubs()
        fsm = ServerStateMachine()
        raft_node = RaftNode(id, state, transport, member_ids, fsm)
        add_service = AddService(id, member_addresses, raft_node)
        raft_service = RaftService(raft_node)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_pb2_grpc.add_AddServiceServicer_to_server(add_service, server)
        raft_pb2_grpc.add_RaftServicer_to_server(raft_service, server)
        members[id] = NodeInfo(transport, fsm, add_service, raft_service, server)


    for member_id, node_info in members.items():
        node_info.add_service.raft_node.start_raft_processing()
        node_info.server.add_insecure_port(member_addresses[member_id])
        node_info.server.start()

    try:
        process_user_commands(members, member_addresses)
    except (KeyboardInterrupt, EOFError):
        pass

    print("Stopping raft processing")
    for member_id, node_info in members.items():
        node_info.add_service.raft_node.stop_raft_processing()
        if node_info.server:
            node_info.server.stop(10)
            node_info.server.wait_for_termination()
        node_info.transport.close_member_stubs()

if __name__ == "__main__":
    main()
