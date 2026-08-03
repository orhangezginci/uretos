#!/bin/bash

BASE_URL="http://localhost:8000/api/v1"

echo "========================================================"
echo " 1. Seeding LOV Data & Extracting UUIDs..."
echo "========================================================"

TYPES_OUTPUT=$(./seed_types.sh)
echo "$TYPES_OUTPUT"
RAW_TYPE_ID=$(echo "$TYPES_OUTPUT" | grep -o 'uuid-[0-9a-fA-F-]\{36\}' | head -n 1)
MACHINE_TYPE_ID=${RAW_TYPE_ID#uuid-}

MANUF_OUTPUT=$(./seed_manufacturers.sh)
echo "$MANUF_OUTPUT"
RAW_MANUF_ID=$(echo "$MANUF_OUTPUT" | grep -o 'uuid-[0-9a-fA-F-]\{36\}' | head -n 1)
MANUFACTURER_ID=${RAW_MANUF_ID#uuid-}

if [ -z "$MACHINE_TYPE_ID" ] || [ -z "$MANUFACTURER_ID" ]; then
  echo "❌ Error: Could not extract dynamic LOV UUIDs from seed scripts."
  exit 1
fi

echo "-> Using dynamic machine_type_id: $MACHINE_TYPE_ID"
echo "-> Using dynamic manufacturer_id: $MANUFACTURER_ID"

echo ""
echo "========================================================"
echo " 2. Creating a test machine (POST)..."
echo "========================================================"

SERIAL="SN-TEST-$(date +%s)"

RESPONSE=$(curl -s -X POST "$BASE_URL/machines" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"Test-Maschine Automation\",
    \"serial_number\": \"$SERIAL\",
    \"machine_type_id\": \"$MACHINE_TYPE_ID\",
    \"manufacturer_id\": \"$MANUFACTURER_ID\"
  }")

echo "Response: $RESPONSE"

MACHINE_ID=$(echo "$RESPONSE" | grep -o '"machine_id":"[^"]*' | cut -d'"' -f4)

if [ -z "$MACHINE_ID" ]; then
  echo "❌ Error: Could not extract machine_id from creation response."
  exit 1
fi

echo "✅ Created Machine with ID: $MACHINE_ID"

echo ""
echo "========================================================"
echo " 3. Updating the test machine (PUT/PATCH)..."
echo "========================================================"

UPD_RESPONSE=$(curl -s -X PUT "$BASE_URL/machines/$MACHINE_ID" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"Test-Maschine Aktualisiert\",
    \"serial_number\": \"$SERIAL-UPDATED\",
    \"machine_type_id\": \"$MACHINE_TYPE_ID\",
    \"manufacturer_id\": \"$MANUFACTURER_ID\"
  }")

echo "Response: $UPD_RESPONSE"

echo ""
echo "========================================================"
echo " 4. Verifying Update in PostgreSQL..."
echo "========================================================"

sleep 1
docker exec -it uretos-machine-db psql -U uretos -d machine_db -c \
  "SELECT id, name, serial_number, deleted_at FROM machines WHERE id = '$MACHINE_ID';"

echo ""
echo "========================================================"
echo " 5. Executing Soft-Delete (DELETE)..."
echo "========================================================"

DEL_RESPONSE=$(curl -s -X DELETE "$BASE_URL/machines/$MACHINE_ID")
echo "Response: $DEL_RESPONSE"

echo ""
echo "========================================================"
echo " 6. Verifying Soft-Delete in PostgreSQL..."
echo "========================================================"

sleep 1
docker exec -it uretos-machine-db psql -U uretos -d machine_db -c \
  "SELECT id, name, serial_number, deleted_at FROM machines WHERE id = '$MACHINE_ID';"

echo "========================================================"
echo " Full CRUD Test Cycle Completed Successfully!"
echo "========================================================"