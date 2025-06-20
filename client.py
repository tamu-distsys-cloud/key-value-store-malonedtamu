import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        self.client_id = nrand()
        self.seq_id = 0

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        args = GetArgs(key)
        while True:
            for srv in self.servers:
                try:
                    reply: GetReply = srv.call("KVServer.Get", args)
                    if reply is not None:
                        return reply.value
                except Exception:
                    # failed, try next server
                    pass

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        self.seq_id += 1
        args = PutAppendArgs(key, value, self.client_id, self.seq_id)
        while True:
            for srv in self.servers:
                try:
                    reply: PutAppendReply = srv.call(f"KVServer.{op}", args)
                    if reply is not None:
                        return reply.value
                except Exception:
                    pass

    def put(self, key: str, value: str):
        return self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
