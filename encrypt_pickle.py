import os
import sys
import pickle
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import getpass


def generate_key_from_password(password: str, salt: bytes = None) -> tuple:
    """Generate a Fernet key from a password using PBKDF2HMAC."""
    if salt is None:
        salt = os.urandom(16)
    
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
    return key, salt


def encrypt_pickle_file(input_file: str, output_file: str, password: str):
    """Encrypt a pickle file using a password."""
    with open(input_file, 'rb') as f:
        data = f.read()
    
    key, salt = generate_key_from_password(password)
    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(data)
    
    with open(output_file, 'wb') as f:
        f.write(salt + b':' + encrypted_data)
    
    print(f"Encrypted file saved to: {output_file}")


def decrypt_pickle_file(input_file: str, output_file: str = None, password: str = None):
    """Decrypt a pickle file using a password."""
    if password is None:
        password = getpass.getpass("Enter decryption key: ")
    
    with open(input_file, 'rb') as f:
        encrypted_data = f.read()
    
    salt, encrypted = encrypted_data.split(b':')
    key, _ = generate_key_from_password(password, salt)
    fernet = Fernet(key)
    
    try:
        decrypted_data = fernet.decrypt(encrypted)
    except Exception as e:
        raise ValueError("Incorrect decryption key")
    
    if output_file:
        with open(output_file, 'wb') as f:
            f.write(decrypted_data)
        print(f"Decrypted file saved to: {output_file}")
    
    return decrypted_data


def load_encrypted_pickle(input_file: str, password: str):
    """Load and decrypt a pickle file directly into memory."""
    with open(input_file, 'rb') as f:
        encrypted_data = f.read()
    
    salt, encrypted = encrypted_data.split(b':')
    key, _ = generate_key_from_password(password, salt)
    fernet = Fernet(key)
    
    try:
        decrypted_data = fernet.decrypt(encrypted)
    except Exception:
        raise ValueError("Incorrect decryption key")
    
    return pickle.loads(decrypted_data)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Encrypt/Decrypt pickle files")
    parser.add_argument("action", choices=["encrypt", "decrypt"], help="Action to perform")
    parser.add_argument("input_file", help="Input file path")
    parser.add_argument("-o", "--output", help="Output file path")
    parser.add_argument("-p", "--password", help="Password (will prompt if not provided)")
    
    args = parser.parse_args()
    
    if args.action == "encrypt":
        password = args.password or getpass.getpass("Enter encryption key: ")
        encrypt_pickle_file(args.input_file, args.output or args.input_file + ".enc", password)
    else:
        password = args.password or getpass.getpass("Enter decryption key: ")
        try:
            decrypt_pickle_file(args.input_file, args.output, password)
            print("Decryption successful!")
        except ValueError as e:
            print(f"Error: {e}")