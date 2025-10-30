from __future__ import annotations
import base64
import json
from typing import Any, Dict, Tuple
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, hmac


class CryptoContext:
    """
    Supports key rotation via epochs. Maintains dictionaries of encryption and
    signing keys per epoch. The current epoch is used for new messages.
    """
    def __init__(self, enc_key: bytes | None = None, sig_key: bytes | None = None):
        # epoch -> keys
        self.enc_keys: Dict[int, Fernet] = {}
        self.sig_keys: Dict[int, bytes] = {}
        self.current_epoch: int = 1
        ek = enc_key or Fernet.generate_key()
        sk = sig_key or Fernet.generate_key()
        self.enc_keys[self.current_epoch] = Fernet(ek)
        self.sig_keys[self.current_epoch] = sk

    def rotate(self, new_enc_key: bytes | None = None, new_sig_key: bytes | None = None) -> int:
        self.current_epoch += 1
        ek = new_enc_key or Fernet.generate_key()
        sk = new_sig_key or Fernet.generate_key()
        self.enc_keys[self.current_epoch] = Fernet(ek)
        self.sig_keys[self.current_epoch] = sk
        return self.current_epoch

    def encrypt_and_sign_with_epoch(self, message: Dict[str, Any]) -> Tuple[str, str, int]:
        epoch = self.current_epoch
        f = self.enc_keys[epoch]
        plaintext = json.dumps(message, separators=(",", ":")).encode("utf-8")
        ciphertext = f.encrypt(plaintext)
        h = hmac.HMAC(self.sig_keys[epoch], hashes.SHA256())
        h.update(ciphertext)
        signature = h.finalize().hex()
        return ciphertext.decode("utf-8"), signature, epoch

    # Backward-compatible API (no epoch exposed)
    def encrypt_and_sign(self, message: Dict[str, Any]) -> Tuple[str, str]:
        ct, sig, _ = self.encrypt_and_sign_with_epoch(message)
        return ct, sig

    def decrypt_and_verify(self, ciphertext: str, signature: str, epoch: int | None = None) -> Dict[str, Any]:
        if epoch is not None:
            f = self.enc_keys.get(epoch)
            sk = self.sig_keys.get(epoch)
            if f is None or sk is None:
                raise ValueError("unknown_epoch")
            ct = ciphertext.encode("utf-8")
            h = hmac.HMAC(sk, hashes.SHA256())
            h.update(ct)
            h.verify(bytes.fromhex(signature))
            plaintext = f.decrypt(ct)
            return json.loads(plaintext.decode("utf-8"))
        # Try current epoch then any others
        epochs = [self.current_epoch] + [e for e in self.enc_keys.keys() if e != self.current_epoch]
        last_err: Exception | None = None
        for ep in epochs:
            try:
                return self.decrypt_and_verify(ciphertext, signature, ep)
            except Exception as e:
                last_err = e
                continue
        raise last_err or ValueError("verify_failed")
