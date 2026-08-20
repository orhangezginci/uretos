import time
import requests

GATEWAY_URL = "http://localhost:8001"

def test_gateway_health():
    print("1. Prüfe Gateway Health-Endpoint...")
    response = requests.get(f"{GATEWAY_URL}/healthz")
    assert response.status_code == 200, f"Health-Check fehlgeschlagen: {response.text}"
    print("   -> Health-Check erfolgreich:", response.json())

def test_token_lifecycle():
    client_id = f"e2e-test-client-{int(time.time())}"
    permissions = ["read:machines", "write:telemetry"]

    print(f"\n2. Generiere Token für Client '{client_id}' (Write-Path / Command)...")
    payload = {
        "client_id": client_id,
        "permissions": permissions
    }
    response = requests.post(f"{GATEWAY_URL}/api/v1/tokens", json=payload)
    
    # Erwartet wird 202 Accepted, da der Befehl asynchron über RabbitMQ verarbeitet wird
    assert response.status_code == 202, f"Token-Erstellung fehlgeschlagen: {response.status_code} - {response.text}"
    data = response.json()
    print("   -> Antwort erhalten:", data)
    
    token_id = data.get("token_id")
    assert token_id, "Keine token_id in der Antwort gefunden!"

    print("\n3. Warte kurz, damit der Command-Worker den Token in der DB persistieren kann...")
    time.sleep(1.5)

    print("\n4. Rufe Token-Liste ab (Read-Path / RPC)...")
    response = requests.get(f"{GATEWAY_URL}/api/v1/tokens")
    assert response.status_code == 200, f"Token-Abfrage fehlgeschlagen: {response.status_code} - {response.text}"
    
    result_data = response.json()
    print(f"   -> Über RPC empfangen: {result_data}")

    # Da der Query-Dienst ein Dictionary {"tokens": [...]} zurückgibt:
    tokens = result_data.get("tokens", []) if isinstance(result_data, dict) else result_data

    # Prüfen, ob der erstellte Token in der Liste auftaucht
    found = False
    for token in tokens:
        if isinstance(token, dict) and (token.get("id") == token_id or token.get("token_id") == token_id):
            found = True
            print(f"   -> Token erfolgreich im Read-Path gefunden: {token}")
            break

    assert found, f"Der erstellte Token mit ID {token_id} wurde im Read-Path nicht gefunden!"
    print("\n✅ E2E-Test für Gateway-Tokens erfolgreich abgeschlossen!")

if __name__ == "__main__":
    try:
        test_gateway_health()
        test_token_lifecycle()
    except Exception as e:
        print(f"\n❌ E2E-Test fehlgeschlagen: {e}")
        exit(1)