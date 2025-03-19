from raft import RaftNode, LocalTransport, PersistentState, AppendLogResponse
from typing import Dict, Optional
import dataclasses

class ServerStateMachine:
    def __init__(self):
        self.sum = 0

    def apply(self, command: str) -> int:
        self.sum += int(command)
        return self.sum

@dataclasses.dataclass
class NodeInfo:
    transport: LocalTransport
    node: RaftNode

def try_send(leader_id: Optional[int], members: Dict[int, NodeInfo], command: str) -> Optional[AppendLogResponse]:
    for i in members.keys():
        if leader_id is None:
            leader_id = i
        try:
            res = members[leader_id].node.append_log(command)
        except RuntimeError as ex:
            leader_id = None
            print(ex)
        if res.success:
            return res

        if res.leader:
            leader_id = res.leader

    return None

def process_user_commands(members: Dict[int, NodeInfo]) -> None:
    while True:
        user_input = input("Enter command:")
        if user_input == "add":
            user_input = input("Enter number:")
            try:
                int(user_input)
            except ValueError:
                print("That's not a valid number")
                continue

            res = try_send(1, members, user_input)
            if res is None:
                print("Failed to add input; server issue")
            else:
                print(f"Total is now {res.result}")

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
            print(f"Stopping node with id {node_id}")
            node_info.node.stop_raft_processing()
            print(f"Node stopped")

        elif user_input == "start node":
            user_input = input("Enter node id:")
            try:
                node_id = int(user_input)
            except ValueError:
                print("That's not a valid number")
                continue
            
            node_info = members[node_id]
            print(f"Starting node with id {node_id}")
            node_info.node.start_raft_processing()
            print(f"Node stopped")

        elif user_input == "leader id":
            for i in members:
                try:
                    print(f"node {i} says leader is {members[i].node.get_leader_id()}")
                except Exception as ex:
                    print(ex)

        elif user_input == "timeout":
            user_input = input("Enter node id:")
            try:
                node_id = int(user_input)
            except ValueError:
                print("That's not a valid number")
                continue
            
            node_info = members[node_id]
            try:
                node_info.node.force_timeout()
            except RuntimeError as ex:
                print(ex)

def main() -> None:
    member_ids = [1, 2, 3]
    members: Dict[int, NodeInfo] = {}
    for id in member_ids:
        state = PersistentState(f"raft_state_{id}.raft")
        transport = LocalTransport()
        fsm = ServerStateMachine()
        raft_node = RaftNode(id, state, transport, member_ids, fsm)
        members[id] = NodeInfo(transport, raft_node)

    # add local transport for all members
    for member_id, node_info in members.items():
        for member_id_peer, node_info_peer in members.items():
            if member_id == member_id_peer:
                continue

            node_info_peer.transport.add_member(member_id, node_info.node)

    for member_id, node_info in members.items():
        node_info.node.start_raft_processing()

    try:
        process_user_commands(members)
    except (KeyboardInterrupt, EOFError):
        print("Stopping raft processing")
        for member_id, node_info in members.items():
            node_info.node.stop_raft_processing()

if __name__ == "__main__":
    main()
