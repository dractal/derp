#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UI_DIR="${ROOT_DIR}/src/derp/studio/ui"
NODE_MODULES_DIR="${UI_DIR}/node_modules"
TEMP_NODE_MODULES_DIR=""

restore_node_modules() {
  if [[ -n "${TEMP_NODE_MODULES_DIR}" ]] && [[ -d "${TEMP_NODE_MODULES_DIR}" ]]; then
    mv "${TEMP_NODE_MODULES_DIR}" "${NODE_MODULES_DIR}"
    rmdir "$(dirname "${TEMP_NODE_MODULES_DIR}")" || true
  fi
}

trap restore_node_modules EXIT

cd "${UI_DIR}"
bun install --frozen-lockfile
bun run build

if [[ -d "${NODE_MODULES_DIR}" ]]; then
  temp_root="$(mktemp -d)"
  TEMP_NODE_MODULES_DIR="${temp_root}/node_modules"
  mv "${NODE_MODULES_DIR}" "${TEMP_NODE_MODULES_DIR}"
fi

cd "${ROOT_DIR}"
uv build
