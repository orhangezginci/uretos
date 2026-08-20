import requests
import json
import time

BASE_URL = "http://localhost:8003"
HEADERS = {"Content-Type": "application/json"}

def test_lov_e2e():
    print("--- [Test 1] Happy Path: Create & Verify LOV Entry ---")
    payload = {
        "category": "machine_type",
        "code": "MILLING_CNC_5AXIS",
        "translations": {
            "en-US": "5-Axis CNC Milling",
            "de-DE": "5-Achs-CNC-Fräse",
            "tr-TR": "5 Eksenli CNC İşleme"
        }
    }
    
    response = requests.post(f"{BASE_URL}/api/v1/lov", data=json.dumps(payload), headers=HEADERS)
    print(f"POST Status: {response.status_code}")
    assert response.status_code == 202, f"Expected 202, got {response.status_code}"
    print("✅ Command accepted.")
    
    print("Waiting for async processing...")
    time.sleep(2)
    
    get_response = requests.get(f"{BASE_URL}/api/v1/lov/machine_type?lang=de-DE")
    print(f"GET Status: {get_response.status_code}")
    assert get_response.status_code == 200
    data = get_response.json()
    assert any(item.get("code") == "MILLING_CNC_5AXIS" for item in data)
    print("✅ Entry successfully verified in DB/Query.")

    print("\n--- [Test 2] Invalid Payload: Missing required 'category' ---")
    invalid_payload_1 = {
        "code": "INVALID_CODE",
        "translations": {"en-US": "Missing Category"}
    }
    response = requests.post(f"{BASE_URL}/api/v1/lov", data=json.dumps(invalid_payload_1), headers=HEADERS)
    print(f"POST Status (Expected 422): {response.status_code}")
    print(f"Response: {response.text}")
    assert response.status_code == 422, f"Expected validation error 422, got {response.status_code}"
    print("✅ Correctly rejected missing field.")

    print("\n--- [Test 3] Invalid Payload: Missing 'translations' dictionary ---")
    invalid_payload_2 = {
        "category": "machine_type",
        "code": "NO_TRANSLATIONS"
    }
    response = requests.post(f"{BASE_URL}/api/v1/lov", data=json.dumps(invalid_payload_2), headers=HEADERS)
    print(f"POST Status (Expected 422): {response.status_code}")
    print(f"Response: {response.text}")
    assert response.status_code == 422, f"Expected validation error 422, got {response.status_code}"
    print("✅ Correctly rejected missing translations.")

    print("\n--- [Test 4] Invalid Payload: Empty body / Bad JSON ---")
    response = requests.post(f"{BASE_URL}/api/v1/lov", data="not-a-json", headers=HEADERS)
    print(f"POST Status (Expected 422): {response.status_code}")
    assert response.status_code == 422, f"Expected validation error 422, got {response.status_code}"
    print("✅ Correctly rejected malformed body.")

    print("\n🎉 All E2E and Validation Tests passed successfully!")

if __name__ == "__main__":
    test_lov_e2e()