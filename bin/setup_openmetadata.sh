#!/usr/bin/env bash
# Register all OpenMetadata ingestion workflows.
# Trino services are created first; dbt workflows attach to them afterwards.
#
# Usage:
#   bin/setup_openmetadata.sh
#
# Override defaults:
#   OM_HOST=http://localhost:8585 OM_ADMIN_PASSWORD=MyPass bin/setup_openmetadata.sh

set -euo pipefail

OM_HOST="${OM_HOST:-http://localhost:8585}"
OM_ADMIN_EMAIL="${OM_ADMIN_EMAIL:-admin@open-metadata.org}"
OM_ADMIN_PASSWORD="${OM_ADMIN_PASSWORD:-Admin1234!}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$(cd "${SCRIPT_DIR}/../projects/openmetadata" && pwd)"

# ── 1. Wait for the server to be ready ───────────────────────────────────────
echo "==> Waiting for OpenMetadata at ${OM_HOST} ..."
for i in $(seq 1 20); do
  if curl -sf "${OM_HOST}/api/v1/system/config/auth" > /dev/null 2>&1; then
    echo "    Server is up."
    break
  fi
  echo "    Attempt ${i}/20 — retrying in 5s..."
  sleep 5
  if [[ $i -eq 20 ]]; then
    echo "ERROR: OpenMetadata did not become ready in time." >&2
    exit 1
  fi
done

# ── 2. Obtain admin JWT ───────────────────────────────────────────────────────
echo "==> Obtaining admin JWT..."
OM_ADMIN_PASSWORD_B64=$(echo -n "${OM_ADMIN_PASSWORD}" | base64)
LOGIN_RESPONSE=$(curl -sf -X POST "${OM_HOST}/api/v1/users/login" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"${OM_ADMIN_EMAIL}\",\"password\":\"${OM_ADMIN_PASSWORD_B64}\"}")
export OM_JWT_TOKEN
OM_JWT_TOKEN=$(echo "${LOGIN_RESPONSE}" | python3 -c "import json,sys; print(json.load(sys.stdin)['accessToken'])")
echo "    JWT obtained."

# ── 3. Run a single workflow ──────────────────────────────────────────────────
run_workflow() {
  local config_file="$1"
  local name
  name="$(basename "${config_file}" .yml)"
  local tmp_rendered="/tmp/om_${name}.yml"
  local container_path="/tmp/${name}.yml"

  echo ""
  echo "==> Workflow: ${name}"
  python3 -c "
import sys, os
content = open(sys.argv[1]).read()
print(content.replace('\${OM_JWT_TOKEN}', os.environ['OM_JWT_TOKEN']))
" "${config_file}" > "${tmp_rendered}"
  docker cp "${tmp_rendered}" openmetadata-ingestion:"${container_path}"
  docker exec openmetadata-ingestion metadata ingest -c "${container_path}"
  echo "    Done: ${name}"
}

# ── 4. Trino warehouse crawl — structural foundation for dbt to attach to ────
# Registers the physical bronze/silver/gold tables so the dbt workflows can
# match their models by FQN. The analytics catalog is intentionally not crawled.
run_workflow "${CONFIG_DIR}/trino-warehouse.yml"

# ── 5. dbt — the primary metadata source: descriptions + lineage + tests ─────
# bronze → silver → gold lineage comes from the dbt ref() graph here.
run_workflow "${CONFIG_DIR}/dbt-bronze.yml"
run_workflow "${CONFIG_DIR}/dbt-silver.yml"
run_workflow "${CONFIG_DIR}/dbt-gold.yml"

echo ""
echo "All workflows complete. Open ${OM_HOST} to explore the catalog."
