import dataclasses
import logging
import math
import queue
import random
import threading
import time
from typing import (Any, Dict, List, Literal, Optional, Protocol, Set, Tuple,
                    Union)

from raft import raft_pb2
from raft.state import LeaderState, PersistentState
from raft.transport.transport_protocol import RaftMessageTransport


class ElectionData:
    def __init__(self, election_term: int):
        self.election_term = election_term
        self.votes_received = 0
        self.voters: Set[int] = set()


class FSM(Protocol):
    def apply(self, command: str) -> Any:
        ...

    def reset(self) -> Any:
        ...


@dataclasses.dataclass
class AppendEntriesRequest:
    event: threading.Event
    request: raft_pb2.AppendEntriesRequest


@dataclasses.dataclass
class AppendEntriesResponse:
    member_id: int
    sent_index: int
    response: raft_pb2.AppendEntriesResponse


@dataclasses.dataclass
class RequestVoteRequest:
    event: threading.Event
    request: raft_pb2.RequestVoteRequest


@dataclasses.dataclass
class RequestVoteResponse:
    member_id: int
    response: raft_pb2.RequestVoteResponse


@dataclasses.dataclass
class AppendLog:
    event: threading.Event
    command: str


class Timeout:
    pass


class SendHeartbeat:
    pass


class Shutdown:
    pass


@dataclasses.dataclass
class CommitLogs:
    commit_index: int


Action = Union[
    AppendEntriesResponse,
    AppendEntriesRequest,
    AppendLog,
    CommitLogs,
    RequestVoteRequest,
    RequestVoteResponse,
    SendHeartbeat,
    Shutdown,
    Timeout,
]
"""These classes can all be in the req_queue"""


@dataclasses.dataclass
class AppendLogResponse:
    success: bool
    leader: Optional[int]
    result: Any


class RaftNode:
    def __init__(
        self,
        id: int,
        state: PersistentState,
        transport: RaftMessageTransport,
        members: List[int],
        fsm: FSM,
        timeout_min_ms: int = 200,
        timeout_max_ms: int = 400,
        heartbeat_interval_ms: int = 100,
    ):
        self._id = id

        format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        self._logger = logging.getLogger(f"raft_{self._id}")
        self._logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(f"raft_{self._id}.log", encoding="utf-8", mode="w")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(format)
        self._logger.addHandler(fh)

        self._state = state
        """Persistent state"""

        self._timeout_min_ms = timeout_min_ms
        self._timeout_max_ms = timeout_max_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms

        self._fsm = fsm
        self._members = members
        self._transport = transport

        self._initialize()

    def _initialize(self) -> None:
        self._heartbeat_thread: Optional[threading.Thread] = None

        self._timeout_deadline: Optional[float] = None

        self._leader_id: Optional[int] = None
        """ID of the current known leader"""

        """Volatile state on all servers (Reinitialized after election)"""

        self._commit_index: int = 0
        """index of highest log entry known to be committed (initialized to 0, increases monotonically)"""
        self._last_applied: int = 0
        """index of highest log entry applied to state machine (initialized to 0, increases monotonically)"""

        self._leader_state = LeaderState()
        """State when this member is the leader"""

        self._role: Literal["F", "C", "L", "S"] = "F"
        """The role of this member. F = follower, C = candidate, L = leader, S = shutdown"""

        self._req_queue: queue.Queue[Action] = queue.Queue()

        self._event_to_append_entries_response: Dict[
            threading.Event, Optional[raft_pb2.AppendEntriesResponse]
        ] = {}
        self._event_to_request_vote_response: Dict[
            threading.Event, Optional[raft_pb2.RequestVoteResponse]
        ] = {}
        self._event_to_append_log_response: Dict[
            threading.Event, Optional[AppendLogResponse]
        ] = {}

        self._log_events: Dict[int, threading.Event] = {}
        """Logs that have been appended but waiting to be committed"""

        self._processing_lock = threading.Lock()

        self._processing_thread: Optional[threading.Thread] = None

        self._accepting_actions = False

    # region process api

    def start_raft_processing(self) -> None:
        with self._processing_lock:
            if self._processing_thread and self._accepting_actions:
                return

            self._logger.warning(f"Start raft process for node {self._id}")
            self._initialize()
            self._fsm.reset()
            self._processing_thread = threading.Thread(target=self._base_loop)
            self._processing_thread.start()
            self._accepting_actions = True

    def stop_raft_processing(self) -> None:
        with self._processing_lock:
            self._accepting_actions = False

            if self._processing_thread is None:
                return

            self._req_queue.put(Shutdown())

        self._processing_thread.join()
        self._processing_thread = None

    def get_leader_id(self) -> Optional[int]:
        with self._processing_lock:
            self._check_running()
            return self._leader_id

    def force_timeout(self) -> None:
        with self._processing_lock:
            self._check_running()
            self._req_queue.put(Timeout())

    # region user api

    def append_log(self, command: str) -> AppendLogResponse:
        """Returns True if the command was applied to the FSM"""
        with self._processing_lock:
            self._check_running()
            event = threading.Event()
            self._req_queue.put(AppendLog(event, command))
            self._event_to_append_log_response[event] = None

        event.wait()
        res = self._event_to_append_log_response.pop(event)
        assert res is not None
        return res

    # endregion

    # region api for raft servicer

    def process_append_entries_request(
        self, request: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.AppendEntriesResponse:
        with self._processing_lock:
            self._check_running()
            event = threading.Event()
            self._req_queue.put(AppendEntriesRequest(event, request))
            self._event_to_append_entries_response[event] = None

        event.wait()
        res = self._event_to_append_entries_response.pop(event)
        assert res is not None
        return res

    def process_request_vote_request(
        self, request: raft_pb2.RequestVoteRequest
    ) -> raft_pb2.RequestVoteResponse:
        with self._processing_lock:
            self._check_running()
            event = threading.Event()
            self._req_queue.put(RequestVoteRequest(event, request))
            self._event_to_request_vote_response[event] = None
        event.wait()
        res = self._event_to_request_vote_response.pop(event)
        assert res is not None
        return res

    # endregion

    # region Base

    def _handle_append_entries_request(
        self, request: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.AppendEntriesResponse:
        # reply false if term < current_term
        if request.term < self._state.current_term:
            return raft_pb2.AppendEntriesResponse(
                term=self._state.current_term, success=False
            )

        self._reset_timeout()

        # if request.term == self.term and we are leader; this should not be possible.
        # only the leader should be sending AppendEntries, and there can be only be one leader per term
        if request.term == self._state.current_term:
            assert self._leader_id != self._id
            self._leader_id = request.leader_id
        else:
            # only other option is request.term > self.term
            # in that case the leader has changed, if we are not already a follower, we need to become one
            self._set_new_term(request.term, request.leader_id, request.leader_id)

        # reply false if log doesn't contain an entry at prev_log_index
        # whose term matches prev_log_term
        if request.prev_log_index > 0:
            prev_log_index_log = self._state.get_log_at(request.prev_log_index)
            if prev_log_index_log is None:
                self._logger.debug(
                    f"We don't have entry at index {request.prev_log_index}!"
                    f"req last index: {request.prev_log_index};"
                    f"req last term: {request.prev_log_index};"
                    f"req entries len: {len(request.entries)};"
                )

                return raft_pb2.AppendEntriesResponse(
                    term=self._state.current_term, success=False
                )
            if prev_log_index_log.term != request.prev_log_term:
                self._logger.debug(
                    "Last log not matched:"
                    f"req last index: {request.prev_log_index};"
                    f"our last index: {prev_log_index_log.index};"
                    f"req last term: {request.prev_log_index};"
                    f"our last term: {prev_log_index_log.term};"
                )
                return raft_pb2.AppendEntriesResponse(
                    term=self._state.current_term, success=False
                )

        # If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it
        # Append any new entries not already in the log
        made_state_changes = False
        for entry in request.entries:
            self._logger.debug(f"Processing entry with index {entry.index}")
            existing_log = self._state.get_log_at(entry.index)

            if existing_log is not None:
                if existing_log.term != entry.term:
                    self._logger.debug(f"Clearing from {entry.index}; term mismatch")
                    self._state.clear_log_from(entry.index)
                    self._logger.debug(f"new log len {self._state.get_log_len()}")
                    made_state_changes = True
                else:
                    self._logger.debug(f"We already have this entry")
            else:
                assert (
                    self._state.get_log_len() + 1 == entry.index
                ), "Index not matching up"
                self._logger.debug(f"Appending entry at {entry.index}")
                self._state.append_log(entry)
                made_state_changes = True

        if made_state_changes:
            self._state.save()

        # If leader_commit > commit_index, set commit_index =
        # min(leader_commit, index of last new entry)
        if request.leader_commit > self._commit_index:
            new_commit_index = min(request.leader_commit, self._state.get_log_len())
            if new_commit_index > self._commit_index:
                self._req_queue.put(CommitLogs(new_commit_index))

        return raft_pb2.AppendEntriesResponse(
            term=self._state.current_term, success=True
        )

    def _handle_request_vote_request(
        self, request: raft_pb2.RequestVoteRequest
    ) -> raft_pb2.RequestVoteResponse:
        # Reply false if term < currentTerm
        if request.term < self._state.current_term:
            self._logger.debug(
                f"Candidate has lower term: ours {self._state.current_term} theirs {request.term}"
            )
            return raft_pb2.RequestVoteResponse(
                term=self._state.current_term, vote_granted=False
            )

        if request.term > self._state.current_term:
            self._logger.debug(
                f"Candidate has higher term: ours {self._state.current_term} theirs {request.term}"
            )
            self._set_new_term(request.term, None)

        # If votedFor is null or candidateId, and candidate's log is at
        # least as up-to-date as receiver's log, grant vote (§5.2, §5.4)

        vote_granted = False
        if self._state.voted_for is None:
            candidate_log_up_to_date = self._check_if_candidate_log_is_up_to_date(
                request.last_log_index, request.last_log_term
            )

            if candidate_log_up_to_date:
                self._logger.debug("Candidate log up to date - providing vote")
                self._set_voted_for(request.candidate_id)
                vote_granted = True
            else:
                self._logger.debug("Candidate log not up to date")
        else:
            self._logger.debug("Already voted this term")

        if vote_granted:
            self._logger.debug(f"Voting for {self._state.voted_for}")
            self._reset_timeout()

        return raft_pb2.RequestVoteResponse(
            term=self._state.current_term, vote_granted=vote_granted
        )

    def _check_if_candidate_log_is_up_to_date(
        self, candidate_last_log_index: int, candidate_last_log_term: int
    ) -> bool:
        """Raft determines which of two logs is more up-to-date
        by comparing the index and term of the last entries in the
        logs. If the logs have last entries with different terms, then
        the log with the later term is more up-to-date. If the logs
        end with the same term, then whichever log is longer is
        more up-to-date."""
        return (
            (last_log := self._state.get_last_log()) is None
            or candidate_last_log_term > last_log.term
            or candidate_last_log_term == last_log.term
            and candidate_last_log_index >= last_log.index
        )

    def _commit_logs(self, action: CommitLogs) -> None:
        """Commits a logs by applying them to the FSM and setting the corresponding event"""
        if self._commit_index >= action.commit_index:
            return

        for i in range(self._commit_index + 1, action.commit_index + 1):
            log = self._state.get_log_at(i)
            assert log is not None
            res = self._fsm.apply(log.command)

            """As leader, client requests are waiting for this. Set event"""
            try:
                event = self._log_events.pop(i)
                self._event_to_append_log_response[event] = AppendLogResponse(
                    True, None, res
                )
                event.set()
            except KeyError:
                pass

        self._commit_index = action.commit_index

    # endregion

    # region Candidate

    def _run_election(self) -> None:
        self._set_new_term(self._state.current_term + 1, self._id)
        self._send_request_vote_to_members()

    def _send_request_vote_to_members(self) -> None:
        """This takes care of sending out vote requests"""
        request = self._create_request_vote_request()
        for member_id in self._members:
            if member_id == self._id:
                continue
            threading.Thread(
                target=self._send_request_vote_request,
                args=(member_id, request),
            ).start()

    def _create_request_vote_request(self) -> raft_pb2.RequestVoteRequest:
        last_log_index = 0
        last_log_term = 0

        if last_log := self._state.get_last_log():
            last_log_index = last_log.index
            last_log_term = last_log.term

        return raft_pb2.RequestVoteRequest(
            term=self._state.current_term,
            candidate_id=self._id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

    def _send_request_vote_request(
        self,
        member_id: int,
        request: raft_pb2.RequestVoteRequest,
    ) -> None:
        try:
            response = self._transport.send_request_vote(member_id, request)
        except Exception as ex:
            self._logger.debug(f"Failed to send request to {member_id}: {ex}")
            return
        else:
            self._req_queue.put(RequestVoteResponse(member_id, response))

    def _handle_request_vote_response(
        self,
        election_data: ElectionData,
        member_id: int,
        response: raft_pb2.RequestVoteResponse,
    ) -> None:
        """Returns true if the vote was provided"""
        if response.term > election_data.election_term:
            self._set_new_term(response.term, None)
            return

        if response.vote_granted and member_id not in election_data.voters:
            election_data.voters.add(member_id)
            election_data.votes_received += 1
            if election_data.votes_received >= self._get_quorum_val():
                self._role = "L"

    # endregion Candidate

    # region Leader

    def _run_heartbeat_loop(self) -> None:
        """Adds a SendHeartbeat req to the queue at a constant interval for the main thread to process"""
        heartbeat_interval_sec = self._heartbeat_interval_ms / 1000
        while self._role == "L":
            curr_time = time.perf_counter()
            next_time = curr_time + heartbeat_interval_sec
            self._req_queue.put(SendHeartbeat())
            time.sleep(max((next_time - time.perf_counter()), 0))

    def _initialize_leader(self) -> None:
        self._leader_id = self._id

        next = self._state.get_log_len() + 1
        for member_id in self._members:
            self._leader_state.match_index[member_id] = 0
            self._leader_state.next_index[member_id] = next

        assert self._heartbeat_thread is None

        self._heartbeat_thread = threading.Thread(target=self._run_heartbeat_loop)
        self._heartbeat_thread.start()

    def _send_append_entries_to_followers(self) -> None:
        assert self._role == "L"
        requests = self._create_append_entries_requests()
        for member_id, sent_index, request in requests:
            threading.Thread(
                target=self._send_append_entries_request,
                args=(member_id, sent_index, request),
            ).start()

    def _create_append_entries_requests(
        self,
    ) -> List[Tuple[int, int, raft_pb2.AppendEntriesRequest]]:
        requests: List[Tuple[int, int, raft_pb2.AppendEntriesRequest]] = []
        for member_id in self._members:
            if member_id == self._id:
                continue

            next_index = self._leader_state.next_index[member_id]

            self._logger.debug(f"Creating append entries request for {member_id}")
            append_entries_request = self._create_append_entries_request(
                self._state.current_term, next_index
            )
            sent_index = next_index + len(append_entries_request.entries) - 1
            requests.append((member_id, sent_index, append_entries_request))

        return requests

    def _create_append_entries_request(
        self, term: int, next_index: int
    ) -> raft_pb2.AppendEntriesRequest:
        prev_log_index = next_index - 1
        prev_log_term = self._state.get_log_term_at(prev_log_index)
        entries = self._state.get_logs_starting_from(next_index, max=10)
        self._logger.debug(
            "AppendRequest:"
            f"term={term};"
            f"id={self._id};"
            f"prev_index={prev_log_index};"
            f"prev_term={prev_log_term};"
            f"entries_len={len(entries)};"
            f"leader_commit={self._commit_index};"
        )
        return raft_pb2.AppendEntriesRequest(
            term=term,
            leader_id=self._id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self._commit_index,
        )

    def _send_append_entries_request(
        self,
        member_id: int,
        sent_index: int,
        append_entries_request: raft_pb2.AppendEntriesRequest,
    ) -> None:
        try:
            response = self._transport.send_append_entries(
                member_id, append_entries_request
            )
        except Exception as ex:
            self._logger.debug(f"Failed to send request to {member_id}: {ex}")
        else:
            self._req_queue.put(AppendEntriesResponse(member_id, sent_index, response))

    def _handle_append_entries_response(
        self,
        member_id: int,
        sent_index: int,
        response: raft_pb2.AppendEntriesResponse,
    ) -> None:
        """Used by leader"""
        if response.term > self._state.current_term:
            self._set_new_term(response.term, None)
            return

        if self._role != "L":
            return

        # handle old responses
        if sent_index < self._leader_state.match_index[member_id]:
            return

        if response.success:
            # If successful: update nextIndex and matchIndex for
            # follower (§5.3)
            self._logger.debug(f"Response from {member_id} success")
            self._leader_state.next_index[member_id] = sent_index + 1
            self._leader_state.match_index[member_id] = sent_index
        else:
            self._logger.debug(
                f"Response from {member_id} indicates fail; decrementing next"
            )
            # decrement next_index so we can try to get them up to speed
            self._leader_state.next_index[member_id] = max(
                self._leader_state.next_index[member_id] - 1, 1
            )

    def _calculate_commit_index(self) -> int:
        # Find the highest log index replicated on a majority of servers
        sorted_match_indexes = sorted(
            self._leader_state.match_index.values(), reverse=True
        )
        commit_index = sorted_match_indexes[self._get_quorum_val() - 1]
        return commit_index

    def _append_log(self, action: AppendLog) -> None:
        if self._role != "L":
            self._logger.debug("We are not leader; notify client")
            self._event_to_append_log_response[action.event] = AppendLogResponse(
                False, self._leader_id, None
            )
            action.event.set()
            return

        self._logger.debug("We are leader; append log and add event")
        index = self._state.get_log_len() + 1
        self._logger.debug(f"Adding log to index {index}")
        self._state.append_log(
            raft_pb2.LogEntry(
                index=index, term=self._state.current_term, command=action.command
            )
        )
        # store event so that we can set it once log is committed
        self._log_events[index] = action.event

        # update match index for ourself
        self._leader_state.match_index[self._id] = index

    # endregion Leader

    # region Processing

    """This section is """

    def _base_loop(self) -> None:
        self._reset_timeout()

        while True:  # might want to add some condition here
            if self._role == "F":
                self._logger.debug("Role Change: Follower")
                self._follower_loop()
            elif self._role == "C":
                self._logger.debug("Role Change: Candidate")
                self._candidate_setup()
                self._candidate_loop()
            elif self._role == "L":
                self._logger.debug("Role Change: Leader")
                self._leader_setup()
                self._leader_loop()
            elif self._role == "S":
                self._logger.debug("Role Change: Shutdown")
                self._shutdown_setup()
                self._shutdown_loop()
                return
            else:
                raise ValueError(f"Invalid role: {self._role}")

    def _handle_default_actions(self, action: Action) -> None:
        if isinstance(action, AppendEntriesResponse):
            self._logger.debug("Processing append entries response")
            self._handle_append_entries_response(
                action.member_id, action.sent_index, action.response
            )

        elif isinstance(action, AppendEntriesRequest):
            self._logger.debug("Processing append entries request")
            append_res = self._handle_append_entries_request(action.request)
            self._event_to_append_entries_response[action.event] = append_res
            action.event.set()

        elif isinstance(action, AppendLog):
            self._logger.debug("Processing append log")
            self._append_log(action)

        elif isinstance(action, CommitLogs):
            self._logger.debug("Processing commit logs")
            self._commit_logs(action)

        elif isinstance(action, RequestVoteRequest):
            self._logger.debug("Processing request vote request")
            vote_res = self._handle_request_vote_request(action.request)
            self._event_to_request_vote_response[action.event] = vote_res
            action.event.set()

        elif isinstance(action, SendHeartbeat):
            self._logger.debug("Processing send heartbeat")
            if self._role == "L":
                commit_index = self._calculate_commit_index()
                if commit_index > self._commit_index:
                    self._logger.debug("commit index changed")
                    self._req_queue.put(CommitLogs(commit_index))
                self._send_append_entries_to_followers()

        elif isinstance(action, Shutdown):
            self._logger.debug("Processing shutdown")
            self._role = "S"

        else:
            self._logger.debug(f"Invalid or unhandled action provided: '{action}'")

    def _follower_loop(self) -> None:
        assert self._timeout_deadline is not None
        while self._role == "F":
            try:
                action = self._req_queue.get(timeout=self._timeout_deadline - time.perf_counter())
            except (queue.Empty, ValueError):  # value error is raised when timeout is negative
                self._logger.warning("Timeout hit!")
                action = Timeout()

            if isinstance(action, Timeout):
                self._logger.debug("Processing timeout")
                # start election process
                self._role = "C"
                return

            if isinstance(action, RequestVoteResponse):
                pass
            else:
                self._handle_default_actions(action)

            self._req_queue.task_done()

    def _candidate_setup(self) -> None:
        self._run_election()
        self._reset_timeout()

    def _candidate_loop(self) -> None:
        election_data = ElectionData(self._state.current_term)
        election_data.votes_received = 1  # vote for ourself
        assert self._timeout_deadline is not None
        while self._role == "C":
            try:
                action = self._req_queue.get(timeout=self._timeout_deadline - time.perf_counter())
            except (queue.Empty, ValueError):
                self._logger.warning("Timeout hit!")
                action = Timeout()

            if isinstance(action, Timeout):
                self._logger.debug("Processing timeout")
                # restart election process
                break

            if isinstance(action, RequestVoteResponse):
                self._handle_request_vote_response(
                    election_data, action.member_id, action.response
                )
            else:
                self._handle_default_actions(action)

            self._req_queue.task_done()

    def _leader_setup(self) -> None:
        self._cancel_timeout()
        self._initialize_leader()

    def _leader_loop(self) -> None:
        while self._role == "L":
            action = self._req_queue.get()

            if isinstance(action, Timeout):
                pass
            if isinstance(action, RequestVoteResponse):
                pass
            else:
                self._handle_default_actions(action)

            self._req_queue.task_done()

        self._heartbeat_thread.join()  # type: ignore
        self._heartbeat_thread = None
        self._reset_timeout()

    def _shutdown_setup(self) -> None:
        self._cancel_timeout()

        self._role = "S"
        if self._heartbeat_thread:
            self._heartbeat_thread.join()

    def _shutdown_loop(self) -> None:
        while True:
            try:
                # adding timeout here just in case
                self._logger.debug("Waiting for queue req")
                action = self._req_queue.get(block=False)
            except queue.Empty:
                self._logger.debug("Queue empty")
                break

            if isinstance(action, Timeout):
                pass
            elif isinstance(action, AppendLog):
                res = AppendLogResponse(False, None, None)
                self._event_to_append_log_response[action.event] = res
                action.event.set()
                continue
            elif isinstance(action, RequestVoteResponse):
                pass
            else:
                self._handle_default_actions(action)

            self._req_queue.task_done()

        # clear all pending events
        for e in self._event_to_append_entries_response:
            e.set()
        for e in self._event_to_append_log_response:
            e.set()
        for e in self._event_to_request_vote_response:
            e.set()
        for log_index in self._log_events:
            self._log_events[log_index].set()

    # endregion

    # region Timeout

    def _reset_timeout(self) -> None:
        self._logger.debug("Resetting timeout")

        election_timeout_ms = random.randint(self._timeout_min_ms, self._timeout_max_ms)
        self._timeout_deadline = time.perf_counter() + (election_timeout_ms / 1000)
        self._logger.debug(f"Next timeout in {election_timeout_ms} ms")

    def _cancel_timeout(self) -> None:
        self._timeout_deadline = None

    # endregion

    # region Misc

    def _check_running(self) -> None:
        if not self._accepting_actions or not self._processing_thread:
            raise RuntimeError("Server not running")

    def _set_new_term(
        self, new_term: int, voted_for: Optional[int], leader_id: Optional[int] = None
    ) -> None:
        if new_term > self._state.current_term:
            self._logger.debug(f"Setting new term {new_term}")
            self._state.set_new_term(new_term, voted_for)
            self._role = "C" if voted_for == self._id else "F"
            self._leader_id = leader_id

    def _get_quorum_val(self) -> int:
        """The number of votes required to reach majority"""
        return math.floor(len(self._members) / 2) + 1

    def _set_voted_for(self, candidate_id: int):
        self._state.set_voted_for(candidate_id)

    # endregion Misc
