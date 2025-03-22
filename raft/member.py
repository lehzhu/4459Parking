import asyncio
import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

import grpc

import raft_pb2
import raft_pb2_grpc
from state import LeaderState, PersistentState


class Member(raft_pb2_grpc.RaftServicer):
    def __init__(
        self,
        id: int,
        state: PersistentState,
        members: Dict[int, str],
        timeout_min_ms: int = 200,
        timeout_max_ms: int = 400,
    ):

        self.id = id

        # self.state = PersistentState(
        #     log_file_path=f"raft_state.{self.id}", reuse_state=False
        # )
        self.state = state
        """Persistent state"""

        self.timeout_min_ms = timeout_min_ms
        self.timeout_max_ms = timeout_max_ms
        self.timeout_task: Optional[asyncio.Task] = None

        self.term_lock = asyncio.Lock()
        """Lock used for critical zones regrading the term and voted_for"""

        self.log_lock = asyncio.Lock()
        """Lock used for critical zones regrading the log"""

        self.members: Dict[int, str] = members
        self.member_channels: Dict[int, grpc.Channel] = {}
        self.member_stubs: Dict[int, raft_pb2_grpc.RaftStub] = {}

        self.leader_id: Optional[int] = None
        """ID of the current known leader"""
        self._is_leader = asyncio.Event()
        """Flag to mark if we are the leader"""

        """Volatile state on all servers (Reinitialized after election)"""

        self.commit_index: int = 0
        """index of highest log entry known to be committed (initialized to 0, increases monotonically)"""
        self.last_applied: int = 0
        """index of highest log entry applied to state machine (initialized to 0, increases monotonically)"""

        self.leader_state: LeaderState = LeaderState()
        """State when this member is the leader"""

        self.index_lock = asyncio.Lock()
        """Lock for leader state index data"""

    def create_member_stubs(self) -> None:
        for member_id, member_address in self.members.items():
            if self.member_stubs.get(member_id) is not None:
                continue

            channel = self.member_channels.get(member_id)
            if channel is None:
                channel = grpc.aio.insecure_channel(member_address)
                self.member_channels[member_id] = channel

            self.member_stubs[member_id] = raft_pb2_grpc.RaftStub(channel)

    async def AppendEntries(
        self, request: raft_pb2.AppendEntriesRequest, context: grpc.ServicerContext
    ) -> raft_pb2.AppendEntriesResponse:
        return await self.handle_append_entries_request(request, context)

    async def RequestVote(
        self, request: raft_pb2.RequestVoteRequest, context: grpc.ServicerContext
    ) -> raft_pb2.RequestVoteResponse:
        return await self.handle_request_vote_request(request, context)

    async def handle_append_entries_request(
        self, request: raft_pb2.AppendEntriesRequest, context: grpc.ServicerContext
    ) -> raft_pb2.AppendEntriesResponse:

        # reply false if term < current_term
        if request.term < self.state.current_term:
            return raft_pb2.AppendEntriesResponse(
                term=self.state.current_term, success=False
            )

        # if request.term == self.term and we are leader; this should not be possible.
        # only the leader should be sending AppendEntries, and there can be only be one leader per term
        if request.term == self.state.current_term:
            assert self.leader_id != self.id
        else:
            # only other option is request.term > self.term
            # in that case the leader has changed, if we are not already a follower, we need to become one
            async with self.term_lock:
                self.set_new_term(request.term, None, request.leader_id)

        self.reset_timeout()

        # reply false if log doesn't contain an entry at prev_log_index
        # whose term matches prev_log_term
        prev_log_index_log = self.state.get_log_at(request.prev_log_index)
        if (
            prev_log_index_log is None
            or prev_log_index_log.term != request.prev_log_index
        ):
            return raft_pb2.AppendEntriesResponse(
                term=self.state.current_term, success=False
            )

        # If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it
        # Append any new entries not already in the log
        async with self.log_lock:
            made_state_changes = False
            for entry in request.entries:
                existing_log = self.state.get_log_at(entry.index)

                if existing_log is not None and existing_log.term != entry.term:
                    self.state.clear_log_from(entry.index)
                    made_state_changes = True

                elif existing_log is None and self.state.get_log_len() == entry.index:
                    self.state.append_log(entry)
                    made_state_changes = True

            if made_state_changes:
                self.state.save()

        # If leader_commit > commit_index, set commit_index =
        # min(leader_commit, index of last new entry)
        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, self.state.get_log_len())

        # TODO: async apply/commit anything that can be

        return raft_pb2.AppendEntriesResponse(
            term=self.state.current_term, success=True
        )

    async def handle_request_vote_request(
        self, request: raft_pb2.RequestVoteRequest, context: grpc.ServicerContext
    ) -> raft_pb2.RequestVoteResponse:

        # critical zone
        async with self.term_lock:

            # Reply false if term < currentTerm
            if request.term < self.state.current_term:
                return raft_pb2.RequestVoteResponse(
                    term=self.state.current_term, vote_granted=False
                )

            if request.term > self.state.current_term:
                self.set_new_term(request.term, None)

            # If votedFor is null or candidateId, and candidate's log is at
            # least as up-to-date as receiver's log, grant vote (§5.2, §5.4)

            vote_granted = False
            if self.state.voted_for is None:
                candidate_log_up_to_date = self.check_if_candidate_log_is_up_to_date(
                    request.last_log_index, request.last_log_term
                )

                if candidate_log_up_to_date:
                    vote_granted = self.set_voted_for(request.candidate_id)

            return raft_pb2.RequestVoteResponse(
                term=self.state.current_term, vote_granted=vote_granted
            )

    def check_if_candidate_log_is_up_to_date(
        self, candidate_last_log_index: int, candidate_last_log_term: int
    ) -> bool:
        """Raft determines which of two logs is more up-to-date
        by comparing the index and term of the last entries in the
        logs. If the logs have last entries with different terms, then
        the log with the later term is more up-to-date. If the logs
        end with the same term, then whichever log is longer is
        more up-to-date."""
        return (
            (last_log := self.state.get_last_log()) is None
            or candidate_last_log_term > last_log.term
            or candidate_last_log_term == last_log.term
            and candidate_last_log_index >= last_log.index
        )

    def set_voted_for(self, candidate_id: int):
        self.state.set_voted_for(candidate_id)

    def reset_timeout(self) -> None:
        if self.timeout_task and not self.timeout_task.done():
            self.timeout_task.cancel()

        election_timeout_ms: int = random.randint(
            self.timeout_min_ms, self.timeout_max_ms
        )
        self.timeout_task = asyncio.create_task(
            self.timeout_handler(election_timeout_ms)
        )

    async def timeout_handler(self, election_timeout_ms: int) -> None:
        await asyncio.sleep(election_timeout_ms)
        await self.start_election()

    async def start_election(self) -> None:
        """Starts an election"""
        async with self.term_lock:
            self.set_new_term(self.state.current_term + 1, self.id)

        # TODO: request vote to all neighbours
        # Need to figure out the timeout part during election.

        """
        TODO:
        When a leader first comes to power,
        it initializes all nextIndex values to the index just after the
        last one in its log (11 in Figure 7).
        """

    async def send_append_entries_to_followers(self, current_term: int) -> None:
        while self._is_leader.is_set():
            curr_time = time.perf_counter()
            heartbeat_interval_sec = 0.1
            next_time = curr_time + heartbeat_interval_sec

            for member_id, next_index in self.leader_state.next_index.items():
                append_entries_request = await self.create_append_entries_request(
                    current_term, next_index
                )
                sent_index = next_index + len(append_entries_request.entries) - 1
                asyncio.create_task(
                    self.send_append_entries_and_handle_response(
                        member_id,
                        sent_index,
                        append_entries_request,
                    )
                )

            await self.calculate_commit_index()

            await asyncio.sleep(max((next_time - time.perf_counter()), 0))

    async def create_append_entries_request(
        self, term: int, next_index: int
    ) -> raft_pb2.AppendEntriesRequest:
        prev_log_index = next_index - 1
        async with self.log_lock:
            prev_log_term = self.state.get_log_term_at(prev_log_index)
            entries = self.state.get_logs_starting_from(next_index, max=10)

        return raft_pb2.AppendEntriesRequest(
            term=term,
            leader_id=self.id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index,
        )

    async def send_append_entries_and_handle_response(
        self,
        member_id: int,
        sent_index: int,
        append_entries_request: raft_pb2.AppendEntriesRequest,
    ) -> None:
        """Sends append entries request and handles reponse
        :param member_id: id of the member to send the request to
        :param sent_index: index of highest sent log
        :param append_entries_request: the request to send
        """
        member_stub = self.member_stubs[member_id]
        try:
            response = await member_stub.AppendEntries(append_entries_request)
        except grpc.RpcError as ex:
            print(f"Failed to send heartbeat to member {member_id}: {ex}")
            return

        await self.handle_append_entries_response(member_id, sent_index, response)

    async def handle_append_entries_response(
        self,
        member_id: int,
        sent_index: int,
        response: raft_pb2.AppendEntriesResponse,
    ) -> None:
        """Should only be run under term_lock"""
        async with self.term_lock:
            if response.term > self.state.current_term:
                self.set_new_term(response.term, None)
                return

        if not self._is_leader.is_set():
            return

        async with self.index_lock:
            # handle old responses
            if sent_index < self.leader_state.match_index[member_id]:
                return

            if response.success:
                # If successful: update nextIndex and matchIndex for
                # follower (§5.3)
                self.leader_state.next_index[member_id] = sent_index
                self.leader_state.match_index[member_id] = (
                    self.leader_state.next_index[member_id] - 1
                )
            else:
                # decrement next_index so we can try to get them up to speed
                self.leader_state.next_index[member_id] = max(
                    self.leader_state.next_index[member_id] - 1, 1
                )

    async def calculate_commit_index(self) -> None:
        async with self.index_lock:
            # Find the highest log index replicated on a majority of servers
            sorted_match_indexes = sorted(
                self.leader_state.match_index.values(), reverse=True
            )
            new_commit_index = sorted_match_indexes[self.get_majority_num() - 1]
            if new_commit_index > self.commit_index:
                self.commit_index = new_commit_index

    def set_new_term(
        self, new_term: int, voted_for: Optional[int], leader_id: Optional[int] = None
    ) -> None:
        """Starts a new term by updating term, voted_for, and updating leader_id. Should only be run under term_lock"""
        if new_term > self.state.current_term:
            self.state.set_new_term(new_term, voted_for)
            self._is_leader.clear()
            self.leader_id = leader_id

    def get_majority_num(self) -> int:
        """The number of votes required to reach majority"""
        # TODO
        return 3

    def temp_all_server_rules(self) -> None:
        # TODO: When should we run this?:
        #  If commitIndex > lastApplied: increment lastApplied, apply
        #  log[lastApplied] to state machine
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            # TODO: apply self.state.log[self.last_applied]
            # commit log / run in state machine
            # need some sort of call back provided by state machine to run the command


MEMBER_ID_TO_ADDRESS: Dict[int, str] = {
    0: "[::]:50050",
    1: "[::]:50051",
    2: "[::]:50052",
    3: "[::]:50053",
    4: "[::]:50054",
}


async def example_start(member_id: int, members: Dict[int, str]) -> None:
    server = grpc.aio.server(ThreadPoolExecutor(max_workers=10))
    state = PersistentState(log_file_path=f"raft_state.{member_id}")
    mem = Member(member_id, state, members)
    raft_pb2_grpc.add_RaftServicer_to_server(mem, server)
    server.add_insecure_port(members[member_id])
    await server.start()
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        pass

    await server.stop(10)


try:
    asyncio.run(example_start(0, MEMBER_ID_TO_ADDRESS))
except KeyboardInterrupt:
    print("server stopped")
