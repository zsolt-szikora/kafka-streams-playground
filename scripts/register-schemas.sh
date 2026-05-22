#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SUBJECTS=(
        info.szikora.kafka.events.Tier
        orders-placed-value
        customer-profiles-value
        orders-per-window-value
        orders-per-window-key
    )
FILES=(
    tier.avsc
    order-placed.avsc
    customer-profile.avsc
    orders-per-window.avsc
    orders-per-window-key.avsc
)

for i in "${!SUBJECTS[@]}"; do
      subject="${SUBJECTS[$i]}"
      file="${FILES[$i]}"
      echo "Registering $subject from $file"

      if [[ "$subject" == "customer-profiles-value" ]]; then
          # extended JSON body that bundles content + references
          body=$(jq -n \
              --arg content "$(cat "$SCRIPT_DIR/../schemas/$file")" \
              '{
                  content: $content,
                  references: [{
                      groupId: null,
                      artifactId: "info.szikora.kafka.events.Tier",
                      version: "1",
                      name: "info.szikora.kafka.events.Tier"
                  }]
              }')
          echo "$body" | curl -sS --fail-with-body -X POST \
              -H "Content-Type: application/create.extended+json" \
              -H "X-Registry-ArtifactType: AVRO" \
              -H "X-Registry-ArtifactId: $subject" \
              --data @- \
              "http://localhost:8080/apis/registry/v2/groups/default/artifacts?ifExists=RETURN_OR_UPDATE" \
              | jq .
      else
          # your existing simple curl, unchanged
          curl -sS --fail-with-body -X POST \
              -H "Content-Type: application/json; artifactType=AVRO" \
              -H "X-Registry-ArtifactId: $subject" \
              --data @"$SCRIPT_DIR/../schemas/$file" \
              "http://localhost:8080/apis/registry/v2/groups/default/artifacts?ifExists=RETURN_OR_UPDATE" \
              | jq '{id, globalId, version}'
      fi
  done