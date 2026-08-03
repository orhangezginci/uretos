#!/bin/bash

API_URL="http://localhost:8000/api/v1"
QUERY_URL="http://localhost:8002/api/v1"

echo "========================================================"
echo " 1. Seeding LOV Data & Extracting UUIDs..."
echo "========================================================"

TYPES_OUTPUT=$(./seed_types.sh)
RAW_TYPE_ID=$(echo "$TYPES_OUTPUT" | grep -o 'uuid-[0-9a-fA-F-]\{36\}' | head -n 1)
MACHINE_TYPE_ID=${RAW_TYPE_ID#uuid-}

MANUF_OUTPUT=$(./seed_manufacturers.sh)
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
echo " 2. Creating a test machine (POST via API)..."
echo "========================================================"

SERIAL="SN-TEST-$(date +%s)"

RESPONSE=$(curl -s -X POST "$API_URL/machines" \
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
echo " 3. Verifying Creation via Query Service (GET)..."
echo "========================================================"
sleep 1 # Kurzer Puffer für asynchrone Event-Verarbeitung

QUERY_RES=$(curl -s "$QUERY_URL/machines?lang=de-DE")
if echo "$QUERY_RES" | grep -q "$MACHINE_ID"; then
  echo "✅ Machine successfully found in Query Read-Model!"
else
  echo "❌ Error: Machine with ID $MACHINE_ID not found in Query service."
  exit 1
fi

echo ""
echo "========================================================"
echo " 4. Updating the test machine (PUT/PATCH via API)..."
echo "========================================================"

UPD_RESPONSE=$(curl -s -X PUT "$API_URL/machines/$MACHINE_ID" \
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
echo " 5. Verifying Update via Query Service (GET)..."
echo "========================================================"
sleep 1

UPDATED_QUERY_RES=$(curl -s "$QUERY_URL/machines?lang=de-DE")
if echo "$UPDATED_QUERY_RES" | grep -q "Test-Maschine Aktualisiert"; then
  echo "✅ Updated machine name verified via Query service!"
else
  echo "❌ Error: Update not reflected in Query service."
  exit 1
fi

echo ""
echo "========================================================"
echo " 6. Executing Soft-Delete (DELETE via API)..."
echo "========================================================"

DEL_RESPONSE=$(curl -s -X DELETE "$API_URL/machines/$MACHINE_ID")
echo "Response: $DEL_RESPONSE"

echo ""
echo "========================================================"
echo " 7. Verifying Soft-Delete via Query Service (GET)..."
echo "========================================================"
sleep 1

FINAL_QUERY_RES=$(curl -s "$QUERY_URL/machines?lang=de-DE")
if ! echo "$FINAL_QUERY_RES" | grep -q "$MACHINE_ID"; then
  echo "✅ Soft-deleted machine successfully hidden from Query Read-Model!"
else
  echo "❌ Error: Soft-deleted machine still appears in standard queries."
  exit 1
fi

echo ""
echo "========================================================"
echo " Full API-driven CRUD & CQRS Test Cycle Completed!"
echo "========================================================"