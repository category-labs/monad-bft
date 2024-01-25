from keystore_module import KDFModule, ChecksumModule, CipherModule
from keystore import CryptoKeystore

from secrets import randbits


def scrypt_test():
    scrypt_params = {
        'salt': randbits(256).to_bytes(32, 'big'),
        'key_len': 32,
        'N': 2**18,
        'r': 8,
        'p': 1,
    }
    aes_params = {
        'iv': randbits(128).to_bytes(16, 'big'),
    }
    ks = CryptoKeystore(KDFModule.KDF.scrypt, scrypt_params,
                        ChecksumModule.ChecksumHashType.SHA256,
                        CipherModule.CipherType.AES_128_CTR, aes_params)
    
    ks.encrypt(b'secret', "password")
    assert(ks.decrypt("password") == b'secret')

    print(ks.convert_to_json())


def pbkdf2_test():
    pbkdf2_params = {
        'salt': randbits(256).to_bytes(32, 'big'),
        'dkLen': 32,
        'count': 2**18,
    }
    aes_params = {
        'iv': randbits(128).to_bytes(16, 'big'),
    }
    ks = CryptoKeystore(KDFModule.KDF.pbkdf2, pbkdf2_params,
                        ChecksumModule.ChecksumHashType.SHA256,
                        CipherModule.CipherType.AES_128_CTR, aes_params)

    ks.encrypt(b'secret', "password")
    assert(ks.decrypt("password") == b'secret')

    print(ks.convert_to_json())


if __name__ == "__main__":
    scrypt_test()
    pbkdf2_test()
