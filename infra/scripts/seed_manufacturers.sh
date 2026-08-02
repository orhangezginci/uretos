#!/bin/bash

API_URL="http://localhost:8003/api/v1/lov"

# Prüfen ob jq installiert ist
if ! command -v jq &> /dev/null; then
    echo "Fehler: 'jq' wird für das Parsen der JSON-Response benötigt."
    exit 1
fi

echo "========================================================"
echo " Seeding Manufacturers via LOV API"
echo "========================================================"

create_manufacturer() {
    local code="$1"
    local name_int="$2"

    RESPONSE=$(curl -s -X POST "$API_URL" \
        -H "Content-Type: application/json" \
        -d "{
            \"code\": \"$code\",
            \"category\": \"manufacturer\",
            \"translations\": {
                \"int\": \"$name_int\"
            }
        }")

    UUID=$(echo "$RESPONSE" | jq -r '.event_id // .id // empty')

    if [ -n "$UUID" ]; then
        echo "uuid-$UUID -> $name_int"
    else
        echo "FEHLER bei '$code': $RESPONSE"
    fi
}

create_manufacturer "DMG_MORI" "DMG MORI"
create_manufacturer "TRUMPF" "TRUMPF"
create_manufacturer "HAAS" "HAAS Automation"
create_manufacturer "MAZAK" "Yamazaki Mazak"

echo "========================================================"