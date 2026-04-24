import sys
import json
import socket
import threading
import time
import math

from blockchain import Blockchain, make_block, print_json
from validation import validate_transaction
from network import send_values, send_bool_response, recv_message


class Node:
    def __init__(self, port, peer_addrs):
        self.port       = port
        self.peer_addrs = peer_addrs
        self.n          = len(peer_addrs) + 1
        self.f          = math.ceil(self.n / 2) - 1

        self.blockchain = Blockchain()

        self._crashed      = set()
        self._crashed_lock = threading.Lock()

        # One event covers both triggers (non-empty pool OR peer asked us)
        self._round_needed = threading.Event()

        self._stop = threading.Event()

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _mark_crashed(self, addr):
        with self._crashed_lock:
            self._crashed.add(addr)

    def _live_peers(self):
        with self._crashed_lock:
            return [a for a in self.peer_addrs if a not in self._crashed]

    def _build_proposal(self):
        last = self.blockchain.last_block()
        return make_block(
            index=last["index"] + 1,
            transactions=self.blockchain.get_pool_transactions(),
            previous_hash=last["current_hash"],
        )

    # ------------------------------------------------------------------ #
    # TCP server                                                           #
    # ------------------------------------------------------------------ #

    def _server_thread(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('0.0.0.0', self.port))
        srv.listen(64)
        srv.settimeout(1.0)
        while not self._stop.is_set():
            try:
                conn, _ = srv.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn,),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break
        try:
            srv.close()
        except Exception:
            pass

    def _handle_connection(self, conn):
        try:
            while not self._stop.is_set():
                msg     = recv_message(conn)
                mtype   = msg.get("type")
                payload = msg.get("payload")
                if mtype == "transaction":
                    self._on_transaction(payload, conn)
                elif mtype == "values":
                    self._on_values(payload, conn)
        except (ConnectionError, OSError, json.JSONDecodeError):
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    # Message handlers                                                     #
    # ------------------------------------------------------------------ #

    def _on_transaction(self, txn, conn):
        ok, _ = validate_transaction(txn, self.blockchain)
        if ok and self.blockchain.add_to_pool(txn):
            print_json({"type": "transaction", "payload": txn})
            sys.stdout.flush()
            self._round_needed.set()
        try:
            send_bool_response(conn, ok)
        except Exception:
            pass

    def _on_values(self, _blocks, conn):
        """
        A peer is asking for our proposal as part of their consensus round.
        We respond immediately with our current proposal, then signal the
        consensus thread that WE also need to run a round (so we commit
        the same decided block).
        """
        proposal = self._build_proposal()
        try:
            send_values(conn, [proposal])
        except Exception:
            pass
        self._round_needed.set()

    # ------------------------------------------------------------------ #
    # Consensus thread – the ONLY place rounds are run                    #
    # ------------------------------------------------------------------ #

    def _consensus_thread(self):
        while not self._stop.is_set():
            self._round_needed.clear()

            # Block until something triggers a round
            while not self._stop.is_set():
                if not self.blockchain.pool_is_empty():
                    break
                if self._round_needed.is_set():
                    break
                self._round_needed.wait(timeout=0.5)

            if self._stop.is_set():
                break

            # Short pause so the peer's exchange has time to receive our
            # response before we start our own outbound exchange
            time.sleep(0.1)

            self._run_round()

    def _run_round(self):
        proposal       = self._build_proposal()
        proposals      = [proposal]
        proposals_lock = threading.Lock()

        def exchange(addr):
            host, port = addr
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2.0)
                s.connect((host, port))
                send_values(s, [proposal])
                response = recv_message(s)
                s.close()
                if response.get("type") == "values":
                    blks = response.get("payload", [])
                    if blks:
                        with proposals_lock:
                            proposals.append(blks[0])
            except Exception:
                self._mark_crashed(addr)

        peers   = self._live_peers()
        threads = [
            threading.Thread(target=exchange, args=(a,), daemon=True)
            for a in peers
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Decision: prefer non-empty blocks; tie-break by smallest hash
        non_empty  = [b for b in proposals if b.get("transactions")]
        candidates = non_empty if non_empty else proposals
        decided    = min(candidates, key=lambda b: b["current_hash"])

        committed = self.blockchain.commit_block(decided)
        if committed:
            print_json(decided)
            sys.stdout.flush()

        self._round_needed.clear()

    # ------------------------------------------------------------------ #
    # Run / Stop                                                           #
    # ------------------------------------------------------------------ #

    def run(self):
        threading.Thread(target=self._server_thread,    daemon=True).start()
        threading.Thread(target=self._consensus_thread, daemon=True).start()
        try:
            while not self._stop.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self._stop.set()

    def stop(self):
        self._stop.set()


# ------------------------------------------------------------------ #
# Entry point                                                          #
# ------------------------------------------------------------------ #

def _load_peers(filepath):
    peers = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            host, port_str = line.rsplit(':', 1)
            peers.append((host, int(port_str)))
    return peers


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <port> <peer-list-file>", file=sys.stderr)
        sys.exit(1)
    port  = int(sys.argv[1])
    peers = _load_peers(sys.argv[2])
    Node(port, peers).run()
