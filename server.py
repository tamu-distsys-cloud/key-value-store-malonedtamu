import logging
import threading
from symbol import continue_stmt
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

def shard_index(key: str, nshards: int) -> int:
    try:
        return int(key) % nshards
    except ValueError:
        return 0

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value, client_id, seq_id):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.seq_id = seq_id

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key):
        self.key = key

class GetReply:
    # Add definitions here if needed
    def __init__(self, value: str = "", err: str = ""):
        self.value = value
        self.err = err

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.data = {} # dict for key val pairs
        self.dupes = {} # dict for client_id and seq_id pairs

    #helper to check if the primary is up
    def primary_up(self, idx:int):
        with self.cfg.mu:
            return idx in self.cfg.running_servers

    def am_i_up(self):
        my_idx = self.cfg.kvservers.index(self)
        with self.cfg.mu:
            return my_idx in self.cfg.running_servers

    def Get(self, args: GetArgs) -> GetReply:
        my_idx = self.cfg.kvservers.index(self)
        primary = shard_index(args.key, self.cfg.nservers)  # shard primary

        # build replica group
        group = []
        for i in range(self.cfg.nreplicas):
            idx = (primary + i) % self.cfg.nservers
            group.append(idx)

        if my_idx not in group:
            return GetReply("", "ErrWrongShard")

        with self.mu:
            val = self.data.get(args.key, "")
        return GetReply(val)

    def Put(self, args: PutAppendArgs) -> PutAppendReply:
        my_idx = self.cfg.kvservers.index(self)
        primary = shard_index(args.key, self.cfg.nservers)  # shard primary

        if not self.am_i_up():
            raise RuntimeError(f"server is down")

        if my_idx != primary: # replica found
            for i in range(1, self.cfg.nreplicas):
                idx = (primary + i) % self.cfg.nservers
                if not self.primary_up(idx):
                    raise RuntimeError(f"server is down")

            if not self.primary_up(primary):
                # if primary is down return nothing
                raise RuntimeError(f"primary down")
            return self.cfg.kvservers[primary].Put(args)  # forward to primary

        with self.mu:
            # Check for duplicate request
            last = self.dupes.get(args.client_id)
            if last and args.seq_id <= last[0]: # duplicate request
                return PutAppendReply(last[1]) # return last reply

            old_val = self.data.get(args.key, "")   # value before update
            self.data[args.key] = args.value
            self.dupes[args.client_id] = (args.seq_id, old_val)  # remember latest request

        for i in range(1, self.cfg.nreplicas):
            idx = (primary + i) % self.cfg.nservers
            self.cfg.kvservers[idx].applyReplica(args.key, self.data[args.key])


        return PutAppendReply(old_val)

    def Append(self, args: PutAppendArgs) -> PutAppendReply:
        my_idx = self.cfg.kvservers.index(self)
        primary = shard_index(args.key, self.cfg.nservers)  # shard primary

        if not self.am_i_up():
            raise RuntimeError(f"server is down")

        if my_idx != primary:
            for i in range(1, self.cfg.nreplicas):
                idx = (primary + i) % self.cfg.nservers
                if not self.primary_up(idx):
                    raise RuntimeError(f"server is down")

            if not self.primary_up(primary):
                # if primary is down return nothing
                raise RuntimeError(f"primary down")
            return self.cfg.kvservers[primary].Append(args)

        with self.mu:
            last = self.dupes.get(args.client_id)
            if last and args.seq_id <= last[0]: # duplicate request
                return PutAppendReply(last[1]) # return last reply

            old_val = self.data.get(args.key, "")
            self.data[args.key] = old_val + args.value
            self.dupes[args.client_id] = (args.seq_id, old_val)  # remember latest request

        for i in range(1, self.cfg.nreplicas):
            idx = (primary + i) % self.cfg.nservers
            self.cfg.kvservers[idx].applyReplica(args.key, self.data[args.key])


        return PutAppendReply(old_val)

    def applyReplica(self, key: str, value: str):
        with self.mu:
            self.data[key] = value