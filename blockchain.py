import json
import hashlib
import threading


GENESIS_PREVIOUS_HASH = "0" * 64


def compute_hash(block_dict):
    """
    Compute SHA-256 of a block dict (excluding current_hash).
    Uses json.dumps with sort_keys=True, indent=2, separators=(',', ': ')
    to match the reference hashing script exactly.
    """
    # Build a copy without current_hash for hashing
    obj = {k: v for k, v in block_dict.items() if k != "current_hash"}
    canonical = json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def make_block(index, transactions, previous_hash):
    """
    Create a block dict with a computed current_hash.
    transactions: list of transaction dicts (each with sender, message, nonce, signature)
    """
    block = {
        "index": index,
        "transactions": transactions,
        "previous_hash": previous_hash,
    }
    block["current_hash"] = compute_hash(block)
    return block


def make_genesis_block():
    """Create the genesis block (index=1, no transactions, fixed previous_hash)."""
    return make_block(1, [], GENESIS_PREVIOUS_HASH)


def print_json(obj):
    """Pretty-print a dict as JSON with sort_keys, indent=2, and correct separators."""
    print(json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')))


class Blockchain:
    """
    Thread-safe blockchain structure.
    Maintains the chain, confirmed sender nonces, and the transaction pool.
    """

    def __init__(self):
        self._lock = threading.Lock()
        genesis = make_genesis_block()
        self._chain = [genesis]
        # confirmed_nonces[sender] = number of confirmed txns from that sender
        self._confirmed_nonces = {}
        # mempool: dict of (sender, nonce) -> transaction dict
        self._pool = {}

    # ------------------------------------------------------------------ #
    # Chain access                                                         #
    # ------------------------------------------------------------------ #

    def last_block(self):
        with self._lock:
            return self._chain[-1]

    def chain_length(self):
        with self._lock:
            return len(self._chain)

    def get_chain(self):
        with self._lock:
            return list(self._chain)

    # ------------------------------------------------------------------ #
    # Transaction pool                                                     #
    # ------------------------------------------------------------------ #

    def confirmed_nonce(self, sender):
        """Return how many txns from sender are confirmed (= expected next nonce)."""
        with self._lock:
            return self._confirmed_nonces.get(sender, 0)

    def pool_has(self, sender, nonce):
        with self._lock:
            return (sender, nonce) in self._pool

    def add_to_pool(self, txn):
        """
        Add a validated transaction to the mempool.
        Returns True if added, False if duplicate key already present.
        """
        key = (txn["sender"], txn["nonce"])
        with self._lock:
            if key in self._pool:
                return False
            self._pool[key] = txn
            return True

    def get_pool_transactions(self):
        """Return a sorted list of all pooled transactions (by sender, then nonce)."""
        with self._lock:
            return sorted(self._pool.values(), key=lambda t: (t["sender"], t["nonce"]))

    def pool_is_empty(self):
        with self._lock:
            return len(self._pool) == 0

    # ------------------------------------------------------------------ #
    # Block commitment                                                     #
    # ------------------------------------------------------------------ #

    def commit_block(self, block):
        """
        Append a decided block to the chain.
        Returns True if committed, False if the index is already present (duplicate).
        """
        with self._lock:
            # Guard: don't commit a block whose index is already in the chain
            if block["index"] <= self._chain[-1]["index"]:
                return False
            self._chain.append(block)
            # Confirm transactions
            for txn in block["transactions"]:
                sender = txn["sender"]
                self._confirmed_nonces[sender] = self._confirmed_nonces.get(sender, 0) + 1
                self._pool.pop((sender, txn["nonce"]), None)

            # Clean pool: drop any entry whose nonce != sender's new expected nonce
            to_remove = [
                (s, n) for (s, n) in self._pool
                if n != self._confirmed_nonces.get(s, 0)
            ]
            for key in to_remove:
                self._pool.pop(key, None)
            return True
