#!/bin/bash

API_URL="http://localhost:8003/api/v1/lov"

# Prüfen ob jq installiert ist
if ! command -v jq &> /dev/null; then
    echo "Fehler: 'jq' wird für das Parsen der JSON-Response benötigt."
    exit 1
fi

echo "========================================================"
echo " Seeding Machine Types via LOV API"
echo "========================================================"

create_type() {
    local code="$1"
    local name_de="$2"
    local name_en="$3"

    RESPONSE=$(curl -s -X POST "$API_URL" \
        -H "Content-Type: application/json" \
        -d "{
            \"code\": \"$code\",
            \"category\": \"machine_type\",
            \"translations\": {
                \"de-DE\": \"$name_de\",
                \"en-US\": \"$name_en\"
            }
        }")

    UUID=$(echo "$RESPONSE" | jq -r '.event_id // .id // empty')

    if [ -n "$UUID" ]; then
        echo "uuid-$UUID -> DE: \"$name_de\" | EN: \"$name_en\""
    else
        echo "FEHLER bei '$code': $RESPONSE"
    fi
}

create_type "CNC_MILL_5AXIS" "5-Achs-CNC-Fräse" "5-Axis CNC Mill"
create_type "CNC_LATHE" "CNC-Drehmaschine" "CNC Lathe"
create_type "LASER_CUTTER" "Laserschneidanlage" "Laser Cutting Machine"
create_type "3D_PRINTER_METAL" "Metall-3D-Drucker" "Metal 3D Printer"

echo "========================================================"