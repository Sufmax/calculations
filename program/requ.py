from proxy_client import ProxyClient

# Initialiser le client
client = ProxyClient.from_config(
    config_path="client_config.json",
    proxy_url="https://proxy-repo.louisgelas-gamer.workers.dev"
)

# Exécuter la requête
response = client.get(
    "https://9d335e56dc6e.ngrok-free.app/test?hello=world",
    headers={
        "User-Agent": "curl-test"
    }
)

# Afficher le résultat
if response.ok:
    print(f"✅ Status: {response.status_code}")
    print(f"📄 Body:\n{response.body}")
else:
    print(f"❌ Erreur: {response.error}")
