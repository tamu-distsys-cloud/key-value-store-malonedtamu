import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

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
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.data = {} # dict for key val pairs
        self.dupes = {} # dict for client_id and seq_id pairs

    def Get(self, args: GetArgs) -> GetReply:
        with self.mu:
            val = self.data.get(args.key, "")
        return GetReply(val)

    def Put(self, args: PutAppendArgs) -> PutAppendReply:
        with self.mu:
            # Check for duplicate request
            last = self.dupes.get(args.client_id)
            if last and args.seq_id <= last[0]: # duplicate request
                return PutAppendReply(last[1]) # return last reply

            old_val = self.data.get(args.key, "")   # value before update
            self.data[args.key] = args.value

        self.dupes[args.client_id] = (args.seq_id, old_val)  # remember latest request
        return PutAppendReply(old_val)

    def Append(self, args: PutAppendArgs) -> PutAppendReply:
        with self.mu:
            last = self.dupes.get(args.client_id)
            if last and args.seq_id <= last[0]: # duplicate request
                return PutAppendReply(last[1]) # return last reply

            old_val = self.data.get(args.key, "")
            self.data[args.key] = old_val + args.value

        self.dupes[args.client_id] = (args.seq_id, old_val)  # remember latest request
        return PutAppendReply(old_val)