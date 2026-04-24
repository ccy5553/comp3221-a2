import re


try:
    import nacl.signing
    import nacl.exceptions
    _NACL = True
except ImportError:
    _NACL = False

try:
    import ed25519 as _ed25519_lib
    _ED25519 = True
except ImportError:
    _ED25519 = False


# ------------------------------------------------------------------ #
# Field format constants                                               #
# ------------------------------------------------------------------ #

SENDER_RE = re.compile(r'^[0-9a-f]{64}$')
SIG_RE    = re.compile(r'^[0-9a-f]{128}$')
MSG_RE    = re.compile(r'^[a-zA-Z0-9 ]{0,70}$')


def _check_fields(txn):
    """Return (True, None) if all fields are well-formed, else (False, reason)."""
    if not isinstance(txn, dict):
        return False, "not a dict"

    required = {"sender", "message", "nonce", "signature"}
    if not required.issubset(txn.keys()):
        return False, "missing fields"

    sender = txn.get("sender", "")
    message = txn.get("message", "")
    nonce = txn.get("nonce")
    sig = txn.get("signature", "")

    if not isinstance(sender, str) or not SENDER_RE.match(sender):
        return False, f"bad sender format: {sender!r}"

    if not isinstance(message, str) or not MSG_RE.match(message):
        return False, f"bad message format: {message!r}"

    if not isinstance(nonce, int) or isinstance(nonce, bool) or nonce < 0:
        return False, f"bad nonce: {nonce!r}"

    if not isinstance(sig, str) or not SIG_RE.match(sig):
        return False, f"bad signature format"

    return True, None


def _verify_signature(txn):
    """
    Verify Ed25519 signature over (sender + message + nonce).
    The signed message is the UTF-8 encoding of sender_hex + message + str(nonce).
    Returns True if valid, False otherwise.
    """
    sender_hex = txn["sender"]
    message    = txn["message"]
    nonce      = txn["nonce"]
    sig_hex    = txn["signature"]

    signed_data = (sender_hex + message + str(nonce)).encode('utf-8')

    try:
        pub_bytes = bytes.fromhex(sender_hex)
        sig_bytes = bytes.fromhex(sig_hex)
    except ValueError:
        return False

    if _NACL:
        try:
            vk = nacl.signing.VerifyKey(pub_bytes)
            vk.verify(signed_data, sig_bytes)
            return True
        except nacl.exceptions.BadSignatureError:
            return False
        except Exception:
            return False
    elif _ED25519:
        try:
            vk = _ed25519_lib.VerifyingKey(pub_bytes)
            vk.verify(sig_bytes, signed_data)
            return True
        except Exception:
            return False
    else:
        raise RuntimeError("No Ed25519 library available (install PyNaCl or ed25519)")


def validate_transaction(txn, blockchain):
    """
    Full validation of a transaction against the current blockchain state.

    Returns (True, None) if valid and should be added to pool.
    Returns (False, reason_str) if invalid.

    blockchain: a Blockchain instance (used for nonce checking).
    """
    # 1. Field format check
    ok, reason = _check_fields(txn)
    if not ok:
        return False, reason

    sender = txn["sender"]
    nonce  = txn["nonce"]

    # 2. Nonce sequence check
    expected_nonce = blockchain.confirmed_nonce(sender)
    if nonce != expected_nonce:
        return False, f"nonce mismatch: expected {expected_nonce}, got {nonce}"

    # 3. Duplicate in pool check
    if blockchain.pool_has(sender, nonce):
        return False, "duplicate transaction in pool"

    # 4. Signature verification
    if not _verify_signature(txn):
        return False, "invalid signature"

    return True, None
