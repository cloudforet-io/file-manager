import base64
import os
import json
import collections

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from spaceone.core import config
from spaceone.core.handler import BaseMutationHandler
from spaceone.secret.error.custom import ERROR_WRONG_ENCRYPT_ALGORITHM

__all__ = ['EncryptMutationHandler']

_encrypt_verb = ['create', 'update_data']
_decrypt_verb = ['get_data']

_SUPPORTED_ENCRYPT_ALGORITHM = [
    'AES'
]

class EncryptMutationHandler(BaseMutationHandler):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.encrypt_algorithm = self.config.get('ENCRYPT_ALGORITHM')
        self._check_config(encrypt_algorithm)

    def request(self, params:dict) -> dict:
        if not (self.transaction.resource == 'Secret' and self.transaction.verb in _encrypt_verb):
            return params
        
        encrypted_data, nonce = self._encrypt(params['data'])
        params['data'] = {
                'encrypted_data': encrypted_data
        }
        params['encrypted'] = True,
        params['encrypt_options'] = {
                'encrypt_algorithm': self.encrypt_algorithm,
                'nonce': nonce,
        }
        return params

    def response(self, result:dict) -> dict:
        if not (self.transaction.resource == 'Secret' and self.transaction.verb in _decrypt_verb):
            return result
        
        if result['encrypted'] is not True:
            return result

        encrypt_options = result.get('encrypt_options', {})
        encrypt_algorithm = encrypt_options.get('encrypt_algorithm')
        if encrypt_algorithm not in _SUPPORTED_ENCRYPT_ALGORITHM:
            raise ERROR_WRONG_ENCRYPT_ALGORITHM(encrypt_algorithm=str(encrypt_algorithm))

        plain_data = self._decrypt(result['data']['encrypted_data'], encrypt_options)

        result = {
            'encrypted': False,
            'encrypt_options': {},
            'data': plain_data
        }

        return result

    def _check_config(self, encrypt_algorithm:str) -> None:
        if encrypt_algorithm not in _SUPPORTED_ENCRYPT_ALGORITHM:
            raise ERROR_WRONG_ENCRYPT_ALGORITHM(encrypt_algorithm)
        
    def _dict_to_b64(self, data: dict) -> bytes:
        return base64.b64encode(json.dumps(data).encode())

    def _b64_to_dict(self, data):
        _data = data if isinstance(data, bytes) else data.encode()
        return json.loads(base64.b64decode(_data).decode())
    
    def _encrypt(self, plain_data:dict) -> (str, str):
        nonce = os.urandom(12)
        data_key = base64.b64decode(self.config.get('encrypt_key'))
        
        aesgcm = AESGCM(data_key)
        del data_key
        encrypted_data = aesgcm.encrypt(nonce, self._dict_to_b64(plain_data), None)
        
        encrypted_data_b64 = base64.b64encode(encrypted_data).decode()
        nonce_b64 = base64.b64encode(nonce).decode()
        return encrypted_data_b64, nonce_b64

    def _decrypt(self, encrypted_data:str, encrypt_options:dict) -> dict:
        nonce = base64.b64decode(encrypt_options['nonce'])
        data_key = base64.b64decode(self.config.get('encrypt_key'))
        
        aesgcm = AESGCM(data_key)
        del data_key

        plain_data = self._b64_to_dict(aesgcm.decrypt(nonce, base64.b64decode(encrypted_data), None))
        
        return plain_data
