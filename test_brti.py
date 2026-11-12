"""Read-only Kalshi BRTI connectivity/response-shape test. Never places trades."""
import os, time, base64, json, sys
import requests
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, ed25519

BASE = 'https://external-api.kalshi.com'
PATH = '/trade-api/v2/cfbenchmarks/values'

def main():
    key_id = os.getenv('KALSHI_API_KEY_ID', '').strip()
    raw = os.getenv('KALSHI_PRIVATE_KEY', '')
    path = os.getenv('KALSHI_PRIVATE_KEY_PATH', '').strip()
    if not key_id or not (raw or path):
        print('SETUP ERROR: Need KALSHI_API_KEY_ID and either KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_PATH')
        return 2
    if path:
        with open(path, 'rb') as f:
            key_bytes = f.read()
    else:
        key_bytes = raw.replace('\\n', '\n').strip().encode()
    try:
        key = serialization.load_pem_private_key(key_bytes, password=None)
    except Exception as e:
        print('KEY ERROR: Unable to parse private key; check multiline PEM formatting. Error type:', type(e).__name__)
        return 2
    timestamp = str(int(time.time() * 1000))
    message = f'{timestamp}GET{PATH}'.encode()
    if isinstance(key, ed25519.Ed25519PrivateKey):
        signature = key.sign(message)
    else:
        signature = key.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    headers = {'KALSHI-ACCESS-KEY': key_id,
               'KALSHI-ACCESS-TIMESTAMP': timestamp,
               'KALSHI-ACCESS-SIGNATURE': base64.b64encode(signature).decode()}
    try:
        response = requests.get(BASE + PATH, params={'id': 'BRTI'}, headers=headers, timeout=12)
        print('HTTP status:', response.status_code)
        if not response.ok:
            print('ACCESS FAILED: Check credentials/entitlement. Response excerpt:', response.text[:300])
            return 1
        data = response.json()
        print('ACCESS SUCCESS')
        print('Response structure (no API credentials):')
        print(json.dumps(data, indent=2)[:3500])
        return 0
    except (requests.RequestException, ValueError) as e:
        print('REQUEST ERROR:', type(e).__name__, str(e)[:250])
        return 1

if __name__ == '__main__':
    sys.exit(main())
