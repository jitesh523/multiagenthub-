from __future__ import annotations
import base64
import json
from typing import Any, Dict, Tuple
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, hmac


class CryptoContext:
    def __init__(self, enc_key: bytes | None = None, sig_key: bytes | None = None):
        self.enc_key = enc_key or Fernet.generate_key()
        self.sig_key = sig_key or Fernet.generate_key()
        self.fernet = Fernet(self.enc_key)

    def encrypt_and_sign(self, message: Dict[str, Any]) -> Tuple[str, str]:
        plaintext = json.dumps(message, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ciphertext = self.fernet.encrypt(plaintext)
        h = hmac.HMAC(self.sig_key, hashes.SHA256())
        h.update(ciphertext)
        signature = base64.urlsafe_b64encode(h.finalize()).decode("utf-8")
        return ciphertext.decode("utf-8"), signature

    def decrypt_and_verify(self, ciphertext: str, signature: str) -> Dict[str, Any]:
        raw = ciphertext.encode("utf-8")
        h = hmac.HMAC(self.sig_key, hashes.SHA256())
        h.update(raw)
        h.verify(base64.urlsafe_b64decode(signature.encode("utf-8")))
        plaintext = self.fernet.decrypt(raw)
        return json.loads(plaintext.decode("utf-8"))
