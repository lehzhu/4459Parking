from typing import Dict

import grpc

from raft import raft_pb2, raft_pb2_grpc


class GrpcTransport:
    def __init__(self, own_id: int, members: Dict[int, str]):
        self._id = own_id
        self._members = members
        self._member_channels: Dict[int, grpc.Channel] = {}
        self._member_stubs: Dict[int, raft_pb2_grpc.RaftStub] = {}

    def send_append_entries(
        self, member_id: int, request: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.AppendEntriesResponse:
        member_stub = self._member_stubs[member_id]
        return member_stub.AppendEntries(request)

    def send_request_vote(
        self, member_id: int, request: raft_pb2.RequestVoteRequest
    ) -> raft_pb2.RequestVoteResponse:
        member_stub = self._member_stubs[member_id]
        return member_stub.RequestVote(request)

    def create_member_stubs(self) -> None:
        for member_id, member_address in self._members.items():
            if self._id == member_id:
                continue
            if self._member_stubs.get(member_id) is not None:
                continue

            channel = self._member_channels.get(member_id)
            if channel is None:
                channel = grpc.insecure_channel(member_address)
                self._member_channels[member_id] = channel

            self._member_stubs[member_id] = raft_pb2_grpc.RaftStub(channel)

    def close_member_stubs(self) -> None:
        for member_channel in self._member_channels.values():
            member_channel.close()

        self._member_channels.clear()
        self._member_stubs.clear()
