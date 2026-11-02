#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "$0")/.." && pwd)
docker build -t txlog:3.2610.1 -f "$ROOT/docker/Dockerfile" "$ROOT"
