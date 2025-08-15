from keystore_module import KDFModule, ChecksumModule, CipherModule

import json


# A cryptographic keystore that is used to encrypt secrets
class CryptoKeystore:
    def __init__(self, kdf_type: KDFModule.KDF, kdf_params,
                 checksum_hash_type: ChecksumModule.ChecksumHashType,
                 cipher_type: CipherModule.CipherType, cipher_params) -> None:
        # Create the three modules of the Keystore
        self.kdf_module = KDFModule(kdf_type, kdf_params)
        self.checksum_module = ChecksumModule(checksum_hash_type)
        self.cipher_module = CipherModule(cipher_type, cipher_params)

    # Encrypt a secret using the provided password
    def encrypt(self, secret: bytes, password: str) -> None:
        # Get the decryption key using the KDF
        decryption_key = self.kdf_module.get_decryption_key(password)
        # Encrypt the secret in the cipher module
        self.cipher_module.encrypt(secret, decryption_key)
        # Update the checksum using the cipher message
        self.checksum_module.update_checksum_message(decryption_key, self.cipher_module.cipher_message)

    # Try to decrypt the cipher message using the provided password
    def decrypt(self, password: str) -> bytes:
        # Get the decryption key using the KDF
        decryption_key = self.kdf_module.get_decryption_key(password)
        # Verify the password using the checksum
        if not self.checksum_module.verify_checksum(decryption_key, self.cipher_module.cipher_message):
            raise ValueError("invalid password")
        # Return the decrypted cipher message
        return self.cipher_module.decrypt(decryption_key)
    
    def convert_to_json(self):
        obj = {
            "ciphertext": self.cipher_module.cipher_message.hex(),
            "checksum": self.checksum_module.checksum.hex(),
            "cipher": self.cipher_module.convert_to_dict(),
            "kdf": self.kdf_module.convert_to_dict(),
            "hash": self.checksum_module.convert_to_dict(),
        }
        return json.dumps(obj)

    def write_to_file(self, file: str):
        with open(file, 'w') as f:
            f.write(self.convert_to_json())


# A keystore with no encryption
class PlainKeystore:
    def __init__(self, private_key: bytes) -> None:
        self.private_key: bytes = private_key
    
    def convert_to_json(self):
        obj = {
            "private_key": self.private_key.hex()
        }

        return json.dumps(obj)

    def write_to_file(self, file: str):
        with open(file, 'w') as f:
            f.write(self.convert_to_json())
