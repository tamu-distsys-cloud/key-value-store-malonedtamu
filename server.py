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
    def __init__(self, key, value):
        self.key = key
        self.value = value

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

    def Get(self, args: GetArgs) -> GetReply:
        with self.mu:
            val = self.data.get(args.key, "")
        return GetReply(val)

    def Put(self, args: PutAppendArgs) -> PutAppendReply:
        with self.mu:
            self.data[args.key] = args.value

        return PutAppendReply(args.value)

    def Append(self, args: PutAppendArgs) -> PutAppendReply:
        with self.mu:
            old = self.data.get(args.key, "")
            self.data[args.key] = old + args.value

        return PutAppendReply(args.value)