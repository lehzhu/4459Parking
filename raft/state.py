import pathlib
from typing import Dict, List, Optional

import msgpack

import raft_pb2


class PersistentState:
    def __init__(self, log_file_path: str = "raft_state", reuse_state: bool = True):
        self._log_file_path = pathlib.Path(log_file_path)

        self._persistent_state = _PersistentState()

        if self._log_file_path.is_file() and reuse_state:
            self._load_persistent_state()
        else:
            # the 0 index is blank since first index is 1
            self._persistent_state.log.append(None)  # type: ignore
            self._save_persistent_state()

    @property
    def current_term(self) -> int:
        return self._persistent_state.current_term

    @property
    def voted_for(self) -> Optional[int]:
        return self._persistent_state.voted_for

    def set_new_term(self, current_term: int, voted_for: Optional[int]) -> None:
        self._persistent_state.voted_for = voted_for
        self._persistent_state.current_term = current_term
        self.save()

    def set_voted_for(self, voted_for: Optional[int]) -> None:
        self._persistent_state.voted_for = voted_for
        self.save()

    def _load_persistent_state(self) -> None:
        with open(self._log_file_path, "rb") as log_file:
            try:
                state = msgpack.unpack(log_file, object_hook=decode_persistent_state)
            except Exception as ex:
                print(f"Failed to load data from {self._log_file_path}: {ex}")
                return

        if not isinstance(state, _PersistentState):
            print(f"{self._log_file_path} does not contain a valid state")
            return

        self._persistent_state = state

    def _save_persistent_state(self) -> None:
        with open(self._log_file_path, "wb") as log_file:
            try:
                msgpack.pack(
                    self._persistent_state, log_file, default=encode_persistent_state
                )
            except Exception as ex:
                print(f"Failed to save data to {self._log_file_path}: {ex}")
                return

    def save(self) -> None:
        self._save_persistent_state()

    def append_log(self, log: raft_pb2.LogEntry) -> None:
        assert log.index == self.get_log_len() + 1
        self._persistent_state.log.append(log)
        # self._save_persistent_state()

    def get_log_at(self, index: int) -> Optional[raft_pb2.LogEntry]:
        try:
            return self._persistent_state.log[index]
        except IndexError:
            return None

    def get_last_log(self) -> Optional[raft_pb2.LogEntry]:
        return self.get_log_at(-1)

    def get_log_term_at(self, index: int) -> int:
        if prev_log := self.get_log_at(index):
            return prev_log.term
        return 0

    def get_logs_starting_from(
        self, index: int, max: int = -1
    ) -> List[raft_pb2.LogEntry]:
        """Get logs from an index and after

        :param index: the index to start from
        :param max: the maximum amount of items to return. less than 0 means no limit
        """
        if index < 1:
            index = 1
        if max >= 0:
            return self._persistent_state.log[index : index + max]
        return self._persistent_state.log[index:]

    def clear_log_from(self, index: int) -> None:
        """clears the log from index on"""
        if index < 1:
            index = 1
        if self.get_log_len() >= index:
            self._persistent_state.log = self._persistent_state.log[:index]
            # self._save_persistent_state()

    def get_log_len(self) -> int:
        return len(self._persistent_state.log) - 1


class _PersistentState:
    """Persistent state on all servers (Updated on stable storage before responding to RPCs)"""

    def __init__(
        self,
        current_term: int = 0,
        voted_for: Optional[int] = None,
        log: Optional[List[raft_pb2.LogEntry]] = None,
    ):
        self.current_term = current_term
        """latest term server has seen (initialized to 0 on first boot, increases monotonically)"""
        self.voted_for = voted_for
        """candidate_id that received vote in current term (or None if none)"""
        if log is None:
            log = []
        self.log: List[raft_pb2.LogEntry] = log
        """log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)"""

    def pack(self):
        return {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [
                log.SerializeToString().decode() if log else None for log in self.log
            ],
        }

    @classmethod
    def unpack(cls, data):
        log = [None]
        for log_i in data["log"]:
            if log_i is None:
                continue
            log.append(raft_pb2.LogEntry.FromString(log_i))

        return cls(data["current_term"], data["voted_for"], log)


def encode_persistent_state(obj):
    if isinstance(obj, _PersistentState):
        return {"_PersistentState": True, "as_dict": obj.pack()}
    return obj


def decode_persistent_state(obj):
    if "_PersistentState" in obj:
        return _PersistentState.unpack(obj["as_dict"])
    return obj


class LeaderState:
    """Volatile state on leaders (Reinitialized after election)"""

    def __init__(self):
        self.next_index: Dict[int, int] = {}
        """for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)"""
        self.match_index: Dict[int, int] = {}
        """for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)"""
