import time
import requests

BASE_URL = "http://localhost:8000/api/v1"
LOV_URL = "http://localhost:8003/api/v1"

def get_lov_id_by_code(category: str, code: str) -> str:
    """Holt die ID eines LOV-Eintrags dynamisch über den Query-Endpoint der LOV-API."""
    for _ in range(10):
        try:
            response = requests.get(f"{LOV_URL}/lov/{category}", params={"lang": "de-DE"})
            if response.status_code == 200:
                items = response.json()
                for item in items:
                    if item.get("code") == code:
                        return item.get("id")
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError(f"Konnte LOV-Eintrag für {category}/{code} nicht finden.")

def run_crud_tests():
    unique_suffix = int(time.time())
    test_serial = f"SN-CNC-{unique_suffix}"
    
    print(f"--- 1. Registriere LOV-Einträge ---")
    
    manufacturer_payload = {
        "category": "manufacturer",
        "code": f"MFR-{unique_suffix}",
        "translations": {"de-DE": "Hersteller X"}
    }
    requests.post(f"{LOV_URL}/lov", json=manufacturer_payload).raise_for_status()

    machine_type_payload = {
        "category": "machine_type",
        "code": f"TYPE-{unique_suffix}",
        "translations": {"de-DE": "CNC-Maschine"}
    }
    requests.post(f"{LOV_URL}/lov", json=machine_type_payload).raise_for_status()

    manufacturer_id = get_lov_id_by_code("manufacturer", f"MFR-{unique_suffix}")
    machine_type_id = get_lov_id_by_code("machine_type", f"TYPE-{unique_suffix}")

    print(f"IDs ermittelt -> Hersteller: {manufacturer_id}, Typ: {machine_type_id}")

    print("\n--- 2. Erstelle Maschine (CREATE) ---")
    machine_payload = {
        "name": f"CNC Fräse {unique_suffix}",
        "serial_number": test_serial,
        "machine_type_id": machine_type_id,
        "manufacturer_id": manufacturer_id
    }
    
    response_machine = requests.post(f"{BASE_URL}/machines", json=machine_payload)
    response_machine.raise_for_status()
    resp_data = response_machine.json()
    
    # Direkte Übernahme der UUIDv4 aus dem Response
    machine_id = resp_data.get("machine_id")
    assert machine_id is not None, "Keine machine_id im Erstellungs-Response enthalten!"
    print(f"[Create] Maschinen-ID erhalten: {machine_id}")

    # Warten, bis das Read-Model die UUID persistiert hat
    print("Warte auf Event-Verarbeitung im Read-Model...")
    found_in_read_model = False
    for i in range(15):
        time.sleep(1)
        machines = requests.get(f"{BASE_URL}/machines").json()
        if any(m["id"] == machine_id for m in machines):
            found_in_read_model = True
            break
        print(f"Versuch {i+1}/15: Warte darauf, dass ID {machine_id} im Read-Model auftaucht...")

    assert found_in_read_model, f"Maschine mit ID {machine_id} ist nach Polling im Read-Model nicht auffindbar!"
    print(f"[Success] Maschinen-ID {machine_id} im Read-Model verifiziert.")

    print("\n--- 3. Aktualisiere Maschine (UPDATE) ---")
    update_payload = {
        "name": f"CNC Fräse {unique_suffix} - Updated",
        "serial_number": f"{test_serial}-MOD",
        "machine_type_id": machine_type_id,
        "manufacturer_id": manufacturer_id
    }
    r = requests.put(f"{BASE_URL}/machines/{machine_id}", json=update_payload)
    assert r.status_code in [200, 202], f"Fehler beim Update: {r.text}"
    
    # Polling für Update anhand der UUID
    updated_machine = None
    for _ in range(15):
        time.sleep(1)
        machines_after_update = requests.get(f"{BASE_URL}/machines").json()
        updated_machine = next((m for m in machines_after_update if m["id"] == machine_id), None)
        if updated_machine and updated_machine["name"] == f"CNC Fräse {unique_suffix} - Updated":
            break
            
    assert updated_machine is not None, f"Die aktualisierte Maschine {machine_id} wurde in der Liste nicht gefunden!"
    print(f"[Update] API-Response verifiziert -> Name: {updated_machine['name']}, Neue SN: {updated_machine['serial_number']}")
    
    assert updated_machine["name"] == f"CNC Fräse {unique_suffix} - Updated"
    assert updated_machine["serial_number"] == f"{test_serial}-MOD"
    print("[Success] Update-Validierung erfolgreich bestanden.")

    print("\n--- 4. Lösche Maschine (SOFT DELETE) ---")
    r = requests.delete(f"{BASE_URL}/machines/{machine_id}")
    assert r.status_code in [200, 202, 204], f"Fehler beim Löschen: {r.text}"
    print(f"[Delete] Soft-Delete Command für Maschine {machine_id} eingereicht.")

    # Polling, bis die UUID aus dem Read-Model verschwunden ist
    deleted_found = True
    for _ in range(15):
        time.sleep(1)
        machines_after_delete = requests.get(f"{BASE_URL}/machines").json()
        deleted_found = any(m["id"] == machine_id for m in machines_after_delete)
        if not deleted_found:
            break
    
    assert not deleted_found, f"FEHLER: Die gelöschte Maschine {machine_id} wird immer noch im Read-Model aufgelistet!"
    print("[Success] Test bestanden: Soft-Delete greift, die UUID ist im Read-Model unsichtbar.")

if __name__ == "__main__":
    run_crud_tests()