from typing import Dict

from raft import RaftNode, raft_pb2


class LocalTransport:
    def __init__(self) -> None:
        self._members: Dict[int, RaftNode] = {}

    def add_member(self, member_id: int, node: RaftNode) -> None:
        self._members[member_id] = node

    def send_append_entries(
        self, member_id: int, request: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.AppendEntriesResponse:
        member_stub = self._members[member_id]
        return member_stub.process_append_entries_request(request)

    def send_request_vote(
        self, member_id: int, request: raft_pb2.RequestVoteRequest
    ) -> raft_pb2.RequestVoteResponse:
        member_stub = self._members[member_id]
        return member_stub.process_request_vote_request(request)
