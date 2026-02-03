from proxy_client import ProxyClient

import subprocess
import re

from datetime import datetime, timezone


def get_blender_version(blender_path="blender"):
    try:
        result = subprocess.run(
            [blender_path, "--version"],
            capture_output=True,
            text=True,
            shell=False
        )
    
        if result.returncode != 0:
            return "result_not_0"
    
        first_line = result.stdout.splitlines()[0]
        match = re.search(r"Blender\s+([\d\.]+)", first_line)
    
        if not match:
            return "No%20problem"
    
        return str(match.group(1))
    except:
        return "Not_installed"

# Initialiser le client
client = ProxyClient.from_config(
    config_path="client_config.json",
    proxy_url="https://proxy-repo.louisgelas-gamer.workers.dev"
)

date = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

blender_version = get_blender_version()

# Exécuter la requête
response = client.get(
    f"https://13eb26bdd6c5.ngrok-free.app/test?blender_version={blender_version}&time={date}",
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








