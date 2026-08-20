import time
import uuid
import requests

MACHINE_API_URL = "http://localhost:8000"

# Bekannte Seed-UUIDs aus euren Seeding-Skripten
VALID_MACHINE_TYPES = [
    "uuid-3ac40134-9c77-4d99-8450-a38b0bfbc214", # 5-Axis CNC Mill
    "uuid-fc67f0b5-59bc-4e15-9a81-a6c4731fdefe", # CNC Lathe
    "uuid-6168264b-3e22-4bc5-8fa7-fc1e171f922f", # Laser Cutting Machine
    "uuid-c51549c1-b1c7-4e43-b7af-261c44867587"  # Metal 3D Printer
]

VALID_MANUFACTURERS = [
    "uuid-151ff68b-c1a0-4d40-8235-5fbc25928cee", # DMG MORI
    "uuid-48d38260-ead6-49eb-b166-18f78647aa4b", # TRUMPF
    "uuid-e6cc40d3-9b41-4b29-87ac-c90396613805", # HAAS Automation
    "uuid-ba2dfd25-ab9b-4c2e-9c63-5f24dc342c3f", # Yamazaki Mazak
    "uuid-38aa12f0-7af0-4ff5-8314-72d7cdba5aa2"  # KUKA
]

def run_api_e2e_test():
    machine_name = f"API-Test-Machine-{int(time.time())}"
    serial_number = f"SN-API-{uuid.uuid4().hex[:8].upper()}"
    
    payload = {
        "name": machine_name,
        "serial_number": serial_number,
        "machine_type_id": VALID_MACHINE_TYPES[0],
        "manufacturer_id": VALID_MANUFACTURERS[0],
        "status_id": str(uuid.uuid4())
    }

    print(f"[API E2E Test] Sending POST request to create machine: {machine_name}")
    response = requests.post(f"{MACHINE_API_URL}/api/v1/machines", json=payload)
    
    if response.status_code not in [200, 201, 202]:
        print(f"❌ Failed to create machine via API: {response.status_code} - {response.text}")
        return

    print("[API E2E Test] Machine creation request accepted. Waiting for event projection...")
    time.sleep(2)

    print("[API E2E Test] Fetching machines list via API...")
    get_response = requests.get(f"{MACHINE_API_URL}/api/v1/machines")
    
    if get_response.status_code != 200:
        print(f"❌ Failed to fetch machines: {get_response.status_code} - {get_response.text}")
        return

    machines = get_response.json()
    found = False
    for m in machines:
        if m.get('serial_number') == serial_number:
            found = True
            break

    if found:
        print("\n✅ API E2E Test PASSED: Machine successfully created via REST and retrieved!")
    else:
        print("\n❌ API E2E Test FAILED: Created machine was not found in the API list response.")

if __name__ == "__main__":
    run_api_e2e_test()