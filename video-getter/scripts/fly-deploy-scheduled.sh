#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v fly >/dev/null 2>&1; then
  echo "fly CLI is required. Install with: brew install flyctl"
  exit 1
fi

if [ ! -f "$ROOT_DIR/fly.toml" ]; then
  echo "fly.toml not found in $ROOT_DIR"
  exit 1
fi

APP_NAME="${FLY_APP:-$(sed -n "s/^app = '\\([^']*\\)'/\\1/p" "$ROOT_DIR/fly.toml" | head -n1)}"
if [ -z "${APP_NAME}" ]; then
  echo "Could not determine Fly app name. Set FLY_APP=<app-name> and retry."
  exit 1
fi

MACHINE_NAME="${FLY_MACHINE_NAME:-youtube-research-daily}"
REGION="${FLY_REGION:-iad}"
SCHEDULE="${FLY_MACHINE_SCHEDULE:-daily}"
VM_CPUS="${FLY_VM_CPUS:-1}"
VM_MEMORY_MB="${FLY_VM_MEMORY_MB:-256}"

RUN_ONCE_VALUE="${RUN_ONCE:-true}"
MAX_DAILY_QUOTA_UNITS_VALUE="${MAX_DAILY_QUOTA_UNITS:-9500}"
SEARCH_PAGE_SIZE_VALUE="${SEARCH_PAGE_SIZE:-50}"
CRAWL_PAGE_SIZE_VALUE="${CRAWL_PAGE_SIZE:-50}"
SEARCH_REGION_CODE_VALUE="${SEARCH_REGION_CODE:-US}"
SEARCH_LANGUAGE_VALUE="${SEARCH_LANGUAGE:-en}"
INTERVAL_MINUTES_VALUE="${INTERVAL_MINUTES:-1440}"

read -r -d '' FIND_MACHINE_BY_NAME_JS <<'EOF' || true
const fs = require("node:fs");
const raw = fs.readFileSync(0, "utf8");
const machines = JSON.parse(raw);
const name = process.argv[1];
const match = machines.find((m) => m?.name === name);
process.stdout.write(match?.id || "");
EOF

existing_id="$(
  fly machine list --app "$APP_NAME" --json \
    | node -e "$FIND_MACHINE_BY_NAME_JS" "$MACHINE_NAME"
)"

if [ -n "$existing_id" ]; then
  echo "Updating scheduled machine $existing_id ($MACHINE_NAME) in app $APP_NAME..."
  fly machine update "$existing_id" \
    --app "$APP_NAME" \
    --yes \
    --dockerfile Dockerfile \
    --schedule "$SCHEDULE" \
    --restart on-fail \
    --vm-cpus "$VM_CPUS" \
    --vm-memory "$VM_MEMORY_MB" \
    --env "RUN_ONCE=$RUN_ONCE_VALUE" \
    --env "MAX_DAILY_QUOTA_UNITS=$MAX_DAILY_QUOTA_UNITS_VALUE" \
    --env "SEARCH_PAGE_SIZE=$SEARCH_PAGE_SIZE_VALUE" \
    --env "CRAWL_PAGE_SIZE=$CRAWL_PAGE_SIZE_VALUE" \
    --env "SEARCH_REGION_CODE=$SEARCH_REGION_CODE_VALUE" \
    --env "SEARCH_LANGUAGE=$SEARCH_LANGUAGE_VALUE" \
    --env "INTERVAL_MINUTES=$INTERVAL_MINUTES_VALUE"
else
  echo "Creating scheduled machine ($MACHINE_NAME) in app $APP_NAME..."
  fly machine run . \
    --app "$APP_NAME" \
    --name "$MACHINE_NAME" \
    --region "$REGION" \
    --schedule "$SCHEDULE" \
    --restart on-fail \
    --vm-cpus "$VM_CPUS" \
    --vm-memory "$VM_MEMORY_MB" \
    --env "RUN_ONCE=$RUN_ONCE_VALUE" \
    --env "MAX_DAILY_QUOTA_UNITS=$MAX_DAILY_QUOTA_UNITS_VALUE" \
    --env "SEARCH_PAGE_SIZE=$SEARCH_PAGE_SIZE_VALUE" \
    --env "CRAWL_PAGE_SIZE=$CRAWL_PAGE_SIZE_VALUE" \
    --env "SEARCH_REGION_CODE=$SEARCH_REGION_CODE_VALUE" \
    --env "SEARCH_LANGUAGE=$SEARCH_LANGUAGE_VALUE" \
    --env "INTERVAL_MINUTES=$INTERVAL_MINUTES_VALUE"
fi

echo
echo "Done. Current machines:"
fly machine list --app "$APP_NAME"
