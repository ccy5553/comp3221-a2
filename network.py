import json
import struct
import socket


# ------------------------------------------------------------------ #
# Low-level framing                                                    #
# ------------------------------------------------------------------ #

def send_message(sock, msg_type, payload):
    """
    Send a length-prefixed JSON message over sock.
    Format: 2-byte big-endian length + JSON body
    """
    body = json.dumps({"type": msg_type, "payload": payload}, separators=(',', ':')).encode('utf-8')
    if len(body) > 0xFFFF:
        raise ValueError(f"Message too large: {len(body)} bytes")
    header = struct.pack('>H', len(body))
    sock.sendall(header + body)


def recv_exact(sock, n):
    """Read exactly n bytes from sock. Returns bytes or raises on EOF/error."""
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed")
        buf += chunk
    return buf


def recv_message(sock):
    """
    Receive one length-prefixed JSON message from sock.
    Returns parsed dict or raises ConnectionError / json.JSONDecodeError.
    """
    header = recv_exact(sock, 2)
    length = struct.unpack('>H', header)[0]
    body = recv_exact(sock, length)
    return json.loads(body.decode('utf-8'))


# ------------------------------------------------------------------ #
# Higher-level helpers                                                 #
# ------------------------------------------------------------------ #

def send_transaction(sock, txn):
    """Send a transaction message."""
    send_message(sock, "transaction", txn)


def send_values(sock, blocks):
    """Send a values message with a list of block dicts."""
    send_message(sock, "values", blocks)


def send_bool_response(sock, value):
    """
    Send a plain true/false JSON response (transaction ack).
    This is a bare JSON boolean, NOT wrapped in {type/payload}.
    """
    body = b'true' if value else b'false'
    header = struct.pack('>H', len(body))
    sock.sendall(header + body)


def recv_bool_response(sock):
    """
    Receive a plain true/false JSON boolean response.
    Returns True or False.
    """
    header = recv_exact(sock, 2)
    length = struct.unpack('>H', header)[0]
    body = recv_exact(sock, length)
    val = json.loads(body.decode('utf-8'))
    return bool(val)
