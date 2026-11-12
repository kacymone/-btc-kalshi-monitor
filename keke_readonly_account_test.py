"""KeKe account API diagnostic. GET requests only. No Telegram, DB, or orders.
Run on Railway with KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY variables.
Do not share API secrets or raw JSON output.
"""
import os, time, base64
import requests
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, ed25519

HOST='https://external-api.kalshi.com'

def read(path,params=None):
    key_id=os.environ['KALSHI_API_KEY_ID'].strip()
    pem=os.environ['KALSHI_PRIVATE_KEY'].replace('\\n','\n').encode()
    key=serialization.load_pem_private_key(pem,password=None)
    timestamp=str(int(time.time()*1000))
    signed=(timestamp+'GET'+path).encode()
    if isinstance(key,ed25519.Ed25519PrivateKey):
        signature=key.sign(signed)
    else:
        signature=key.sign(signed,padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.DIGEST_LENGTH),hashes.SHA256())
    headers={'KALSHI-ACCESS-KEY':key_id,'KALSHI-ACCESS-TIMESTAMP':timestamp,
             'KALSHI-ACCESS-SIGNATURE':base64.b64encode(signature).decode()}
    response=requests.get(HOST+path,params=params,headers=headers,timeout=12)
    response.raise_for_status()
    return response.json()

def main():
    print('KeKe READ-ONLY account diagnostic. No trading, no DB writes.',flush=True)
    for name,path in [('positions','/trade-api/v2/portfolio/positions'),
                      ('fills','/trade-api/v2/portfolio/fills')]:
        try:
            obj=read(path,{'limit':10})
            field='market_positions' if name=='positions' else 'fills'
            items=obj.get(field)
            if not isinstance(items,list):
                print(f'{name}: HTTP success but unexpected response structure; manual review needed',flush=True)
            else:
                print(f'{name}: AUTHORIZED; received {len(items)} records in first page; '
                      f'fields={sorted(items[0].keys()) if items else "none (empty page)"}',flush=True)
        except requests.HTTPError as exc:
            print(f'{name}: HTTP {exc.response.status_code} (not authorized or endpoint error)',flush=True)
        except Exception as exc:
            print(f'{name}: TEST FAILED ({type(exc).__name__}); no secrets printed',flush=True)
    print('Diagnostic finished. No orders placed.',flush=True)

if __name__=='__main__':main()
