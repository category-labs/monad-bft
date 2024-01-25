from Crypto.Protocol.KDF import PBKDF2, scrypt
from Crypto.Hash import SHA256 as SHA256
from Crypto.Cipher import AES as AES

from enum import Enum

from typing import Any, Dict


# A Keystore module that represents a Key Derivation Function
class KDFModule:
    # Types of KDFs
    class KDF(Enum):
        scrypt = 1
        pbkdf2 = 2

    def __init__(self, kdf_type: KDF, params: Dict[str, Any]) -> None:
        # Type of KDF used by this instance
        self.kdf_type: KDFModule.KDF = kdf_type
        # KDF parameters
        self.params: Dict[str, Any] = params

    # Generate the decryption key using the password
    def get_decryption_key(self, password: str):
        if self.kdf_type == KDFModule.KDF.scrypt:
            dk = scrypt(password=password, **self.params)
            # scrypt may return a Tuple
            return dk if isinstance(dk, bytes) else dk[0]
        elif self.kdf_type == KDFModule.KDF.pbkdf2:
            dk = PBKDF2(password=password, **self.params)
            return dk
        else:
            raise TypeError("invalid kdf")

    def convert_to_dict(self) -> Dict:
        kdf_name = ""
        if self.kdf_type == KDFModule.KDF.scrypt:
            kdf_name = "scrypt"
        elif self.kdf_type == KDFModule.KDF.pbkdf2:
            kdf_name = "pbkdf2"
        else:
            raise TypeError("invalid kdf")

        # Convert all the byte parameters to hex
        params = dict()
        for (k, v) in self.params.items():
            params[k] = v.hex() if isinstance(v, bytes) else v

        return {
            "kdf_name": kdf_name,
            "params": {
                **params,
            },
        }


# A Keystore module that represents checksum verifiction
class ChecksumModule:
    # Types of hash functions for calculating checksum
    class ChecksumHashType(Enum):
        SHA256 = 1

    def __init__(self, checksum_hash_type: ChecksumHashType) -> None:
        # Type of hash function used by this instance
        self.checksum_hash_type: ChecksumModule.ChecksumHashType = checksum_hash_type
        # The checksum message to verify with on decryption
        self.checksum: bytes = bytes()

    # Update the checksum with the decryption key and cipher message
    def update_checksum_message(self, decryption_key: bytes, cipher_message: bytes) -> None:
        if self.checksum_hash_type == ChecksumModule.ChecksumHashType.SHA256:
            # Hash of the concatenated decryption key part and the cipher message
            self.checksum = SHA256.new(decryption_key[16:32] + cipher_message).digest()
        else:
            raise TypeError("invalid hash function")

    # Verify the checksum with the decryption key and cipher message
    def verify_checksum(self, decryption_key: bytes, cipher_message: bytes) -> bool:
        pre_image = SHA256.new(decryption_key[16:32] + cipher_message).digest()
        return pre_image == self.checksum
    
    def convert_to_dict(self) -> Dict:
        checksum_hash = ""
        if self.checksum_hash_type == ChecksumModule.ChecksumHashType.SHA256:
            checksum_hash = "SHA256"
        else:
            raise TypeError("invalid hash function")
        
        return {
            "checksum_hash": checksum_hash,
            "checksum": self.checksum.hex(),
        }


# A Keystore module that represents cipher encryption of the secret
class CipherModule:
    # Types of ciphers used for encryption
    class CipherType(Enum):
        AES_128_CTR = 1

    def __init__(self, cipher_type: CipherType, cipher_params: Dict[str, Any]) -> None:
        # Type of cipher used by this instance
        self.cipher_type: CipherModule.CipherType = cipher_type
        # Initialization vector for the cipher
        self.iv: bytes = cipher_params['iv']
        # Encrypted cipher message of the secret
        self.cipher_message: bytes = bytes()

    # Encrypt the secret uinsg the encryption key
    def encrypt(self, secret: bytes, encryption_key: bytes) -> None:
        if self.cipher_type == CipherModule.CipherType.AES_128_CTR:
            # Get an instance of AES_128 in counter mode with IV
            cipher = AES.new(key=encryption_key[:16], mode=AES.MODE_CTR,
                             initial_value=self.iv, nonce=b'')
            self.cipher_message = cipher.encrypt(secret)
        else:
            raise TypeError("invalid cipher type")

    # Decrypt the cipher message using the decryption key
    def decrypt(self, decryption_key: bytes) -> bytes:
        if self.cipher_type == CipherModule.CipherType.AES_128_CTR:
            # Get an instance of AES_128 in counter mode with IV
            cipher = AES.new(key=decryption_key[:16], mode=AES.MODE_CTR,
                             initial_value=self.iv, nonce=b'')
            return cipher.decrypt(self.cipher_message)
        else:
            raise TypeError("invalid cipher type")
        
    def convert_to_dict(self) -> Dict:
        cipher_function = ""
        if self.cipher_type == CipherModule.CipherType.AES_128_CTR:
            cipher_function = "AES_128_CTR"
        else:
            raise TypeError("invalid cipher type")
        
        return {
            "cipher_function": cipher_function,
            "cipher_message": self.cipher_message.hex(),
            "params": {
                # Convert IV bytes to hex
                "iv": self.iv.hex()
            }
        }
