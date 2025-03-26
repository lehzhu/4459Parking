from typing import Protocol

from raft import raft_pb2


class RaftMessageTransport(Protocol):
    def send_append_entries(
        self, member_id: int, request: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.AppendEntriesResponse:
        ...

    def send_request_vote(
        self, member_id: int, request: raft_pb2.RequestVoteRequest
    ) -> raft_pb2.RequestVoteResponse:
        ...
