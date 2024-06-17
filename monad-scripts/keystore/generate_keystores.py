## This tool is just used for testing purposes.
## It is insecure and do not use in production.

from blspy import PrivateKey as BLSPrivateKey
from Crypto.Hash import SHA256

from getpass import getpass

from keystore_module import KDFModule, ChecksumModule, CipherModule
from keystore import CryptoKeystore

from secrets import randbits

from unicodedata import normalize

import argparse

# Control codes include C0 codes + Delete + C1 codes
CONTROL_CODES = list(range(0x00, 0x20)) + [0x7F] + list(range(0x80, 0xA0))


def generate_secret_key() -> bytes:
    return SHA256.new(randbits(256).to_bytes(32, 'big')).digest()


def get_password():
    password = getpass("Enter password:")
    # Normalize
    password = normalize("NFKD", password)
    # Remove the control codes
    password = password.translate({ord(ch): None for ch in str(CONTROL_CODES)})

    return password


def generate_keystores(args):
    bls_key = generate_secret_key()
    secp_key = generate_secret_key()

    if args.password is None:
        password = get_password()
    else:
        password = args.password

    bls_path = "bls.json" if args.bls_keystore_path is None else args.bls_keystore_path
    secp_path = "secp.json" if args.secp_keystore_path is None else args.secp_keystore_path

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
    bls_ks = CryptoKeystore(KDFModule.KDF.scrypt, scrypt_params,
                            ChecksumModule.ChecksumHashType.SHA256,
                            CipherModule.CipherType.AES_128_CTR, aes_params)
    secp_ks = CryptoKeystore(KDFModule.KDF.scrypt, scrypt_params,
                             ChecksumModule.ChecksumHashType.SHA256,
                             CipherModule.CipherType.AES_128_CTR, aes_params)

    bls_ks.encrypt(bls_key, password)
    secp_ks.encrypt(secp_key, password)

    bls_ks.write_to_file(bls_path)
    secp_ks.write_to_file(secp_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Keystore generation options.")
    parser.add_argument("--password", type=str, help="Password to encrypt the keys")
    parser.add_argument("--bls-keystore-path", type=str, help="Output file path for BLS key")
    parser.add_argument("--secp-keystore-path", type=str, help="Output file path for SECP key")
    
    args = parser.parse_args()
    generate_keystores(args)
    
