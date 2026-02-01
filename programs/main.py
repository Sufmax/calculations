#!/usr/bin/env python3
"""proxy_client.py - Client proxy universel pour la VM"""

import json
import hmac
import hashlib
import time
import uuid
import requests
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass

@dataclass
class ProxyResponse:
    """Réponse du proxy"""
    success: bool
    status_code: int
    headers: Dict[str, str]
    body: str
    error: Optional[str] = None
    
    @property
    def json(self) -> Any:
        """Parse le body en JSON"""
        try:
            return json.loads(self.body)
        except:
            return None
    
    @property
    def ok(self) -> bool:
        return self.success and 200 <= self.status_code < 300


class ProxyClient:
    """Client HTTP utilisant un proxy distant"""
    
    def __init__(
        self,
        proxy_url: str,
        api_key: str,
        hmac_secret: str,
        timeout: int = 60
    ):
        """
        Args:
            proxy_url: URL du proxy (ex: https://mon-proxy.workers.dev)
            api_key: Clé API
            hmac_secret: Secret HMAC pour les signatures
            timeout: Timeout par défaut en secondes
        """
        self.proxy_url = proxy_url.rstrip('/')
        self.api_key = api_key
        self.hmac_secret = hmac_secret
        self.timeout = timeout
    
    @classmethod
    def from_config(cls, config_path: str, proxy_url: str) -> 'ProxyClient':
        """Crée un client depuis un fichier de config"""
        with open(config_path) as f:
            config = json.load(f)
        return cls(
            proxy_url=proxy_url,
            api_key=config['api_key'],
            hmac_secret=config['hmac_secret']
        )
    
    def _sign_request(self, timestamp: int, nonce: str, body: str) -> str:
        """Génère la signature HMAC"""
        payload = f"{timestamp}:{nonce}:{body}"
        return hmac.new(
            self.hmac_secret.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()
    
    def _make_request(self, payload: dict) -> ProxyResponse:
        """Envoie une requête au proxy"""
        timestamp = int(time.time())
        nonce = uuid.uuid4().hex
        body = json.dumps(payload)
        signature = self._sign_request(timestamp, nonce, body)
        
        headers = {
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key,
            'X-Timestamp': str(timestamp),
            'X-Nonce': nonce,
            'X-Signature': signature
        }

        # 🔍 DEBUG - Ajoute ces lignes
        print(f"🔍 API Key (début): {self.api_key[:10]}...")
        print(f"🔍 URL: {self.proxy_url}/proxy")
        
        try:
            response = requests.post(
                f"{self.proxy_url}/proxy",
                headers=headers,
                data=body,
                timeout=self.timeout
            )
            
            if response.status_code == 401:
                return ProxyResponse(
                    success=False,
                    status_code=401,
                    headers={},
                    body='',
                    error='Authentication failed'
                )
            
            result = response.json()
            
            return ProxyResponse(
                success=result.get('success', False),
                status_code=result.get('status_code', 0),
                headers=result.get('headers', {}),
                body=result.get('body', ''),
                error=result.get('error')
            )
            
        except requests.exceptions.Timeout:
            return ProxyResponse(
                success=False, status_code=0, headers={}, body='',
                error='Proxy request timeout'
            )
        except requests.exceptions.RequestException as e:
            return ProxyResponse(
                success=False, status_code=0, headers={}, body='',
                error=f'Proxy connection error: {str(e)}'
            )
        except Exception as e:
            return ProxyResponse(
                success=False, status_code=0, headers={}, body='',
                error=f'Unexpected error: {str(e)}'
            )
    
    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Union[str, bytes]] = None,
        json_data: Optional[dict] = None,
        timeout: Optional[int] = None
    ) -> ProxyResponse:
        """
        Effectue une requête HTTP via le proxy
        
        Args:
            method: GET, POST, PUT, DELETE, PATCH, etc.
            url: URL de destination
            headers: Headers HTTP optionnels
            data: Données brutes (string/bytes)
            json_data: Données JSON (sera converti en string)
            timeout: Timeout en secondes
        
        Returns:
            ProxyResponse avec le résultat
        """
        body = None
        if json_data is not None:
            body = json_data
            if headers is None:
                headers = {}
            headers.setdefault('Content-Type', 'application/json')
        elif data is not None:
            body = data if isinstance(data, str) else data.decode()
        
        payload = {
            'method': method.upper(),
            'url': url,
            'headers': headers or {},
            'body': body,
            'timeout': (timeout or self.timeout) * 1000  # en ms
        }
        
        return self._make_request(payload)
    
    # Méthodes raccourcies
    def get(self, url: str, **kwargs) -> ProxyResponse:
        return self.request('GET', url, **kwargs)
    
    def post(self, url: str, **kwargs) -> ProxyResponse:
        return self.request('POST', url, **kwargs)
    
    def put(self, url: str, **kwargs) -> ProxyResponse:
        return self.request('PUT', url, **kwargs)
    
    def delete(self, url: str, **kwargs) -> ProxyResponse:
        return self.request('DELETE', url, **kwargs)
    
    def patch(self, url: str, **kwargs) -> ProxyResponse:
        return self.request('PATCH', url, **kwargs)


# ============================================================
# EXEMPLES D'UTILISATION
# ============================================================

if __name__ == "__main__":
    # Méthode 1: Configuration manuelle
    # client = ProxyClient(
    #     proxy_url="https://proxy-repo.louisgelas-gamer.workers.dev/",  # À remplacer
    #     api_key="VOTRE_API_KEY",                       # À remplacer
    #     hmac_secret="VOTRE_HMAC_SECRET"                # À remplacer
    # )
    
    # Méthode 2: Depuis fichier config
    client = ProxyClient.from_config(
        config_path="client_config.json",
        proxy_url="https://proxy-repo.louisgelas-gamer.workers.dev/"
    )
    
    print("=" * 60)
    print("🧪 Test du proxy")
    print("=" * 60)
    
    # Test GET
    print("\n📥 Test GET...")
    resp = client.get("https://httpbin.org/get")
    if resp.ok:
        print(f"✅ Status: {resp.status_code}")
        print(f"📄 Body (extrait): {resp.body[:200]}...")
    else:
        print(f"❌ Erreur: {resp.error}")
    
    # Test POST JSON
    print("\n📤 Test POST JSON...")
    resp = client.post(
        "https://httpbin.org/post",
        json_data={"message": "Hello from VM!", "timestamp": time.time()}
    )
    if resp.ok:
        print(f"✅ Status: {resp.status_code}")
        data = resp.json
        if data:
            print(f"📄 JSON reçu: {data.get('json', {})}")
    else:
        print(f"❌ Erreur: {resp.error}")
    
    # Test avec headers custom
    print("\n🔧 Test avec headers custom...")
    resp = client.get(
        "https://httpbin.org/headers",
        headers={
            "X-Custom-Header": "TestValue",
            "User-Agent": "VM-Proxy-Client/1.0"
        }
    )
    if resp.ok:
        print(f"✅ Headers envoyés confirmés")
    else:
        print(f"❌ Erreur: {resp.error}")
    
    print("\n" + "=" * 60)
    print("✅ Tests terminés")
