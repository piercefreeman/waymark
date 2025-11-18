#!/usr/bin/env bash
set -euo pipefail

if command -v protoc >/dev/null 2>&1; then
  echo "protoc already installed"
  exit 0
fi

apt-get update
apt-get install -y protobuf-compiler
