#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPOSITORY_ROOT"

IMAGE="${AIWS_VERIFY_IMAGE:-ai-job-workspace-backend:runtime-verification}"
OUTPUT="${AIWS_VERIFY_PDF_OUTPUT:-artifacts/realistic-resume.pdf}"
PYTHON_SCRIPT="scripts/verify_pdf_interview.py"

docker_command=()
windows_docker=false
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker_command=(docker)
elif [[ -x "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe" ]] &&
  "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe" info >/dev/null 2>&1; then
  docker_command=("/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe")
  windows_docker=true
else
  echo "Docker is unavailable. Start Docker Desktop and enable this WSL distribution." >&2
  exit 1
fi

docker_source_path() {
  if "$windows_docker"; then
    wslpath -w "$1"
  else
    printf '%s\n' "$1"
  fi
}

docker_build_context="$REPOSITORY_ROOT"
if "$windows_docker"; then
  docker_build_context="$(wslpath -w "$REPOSITORY_ROOT")"
fi
"${docker_command[@]}" build --tag "$IMAGE" "$docker_build_context"

uv run python "$PYTHON_SCRIPT" --contract-only
uv run python "$PYTHON_SCRIPT" --config config.jsonc --skip-contract --skip-pdf

container_id="$(
  "${docker_command[@]}" create \
    --entrypoint python \
    "$IMAGE" \
    /tmp/verify_pdf_interview.py \
    --config /tmp/example.jsonc \
    --skip-contract \
    --skip-provider \
    --pdf-output /tmp/realistic-resume.pdf \
    --resume-json-output /tmp/realistic-resume-input.json
)"
cleanup() {
  "${docker_command[@]}" rm -f "$container_id" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${docker_command[@]}" cp \
  "$(docker_source_path "$PYTHON_SCRIPT")" \
  "$container_id":/tmp/verify_pdf_interview.py
"${docker_command[@]}" cp \
  "$(docker_source_path example.jsonc)" \
  "$container_id":/tmp/example.jsonc
"${docker_command[@]}" start -a "$container_id"

mkdir -p "$(dirname "$OUTPUT")"
"${docker_command[@]}" cp \
  "$container_id":/tmp/realistic-resume.pdf \
  "$(docker_source_path "$OUTPUT")"
"${docker_command[@]}" cp \
  "$container_id":/tmp/realistic-resume-input.json \
  "$(docker_source_path artifacts/realistic-resume-input.json)"

uv run python - "$OUTPUT" <<'PY'
import hashlib
import sys
from pathlib import Path

from pypdf import PdfReader

path = Path(sys.argv[1])
content = path.read_bytes()
reader = PdfReader(path)
if not content.startswith(b"%PDF-") or not reader.pages:
    raise SystemExit("rendered artifact is not a readable PDF")
print(
    {
        "pdf_path": str(path.resolve()),
        "bytes": len(content),
        "pages": len(reader.pages),
        "sha256": hashlib.sha256(content).hexdigest(),
        "text": [page.extract_text() for page in reader.pages],
    }
)
PY

uv run pytest -q \
  tests/test_v2_interview_http.py \
  tests/test_v2_interview_realtime.py \
  tests/test_v2_interview_application.py \
  tests/test_v2_interview_worker.py \
  tests/test_v2_interview_report_provider.py \
  tests/test_v2_interview_persistence.py \
  tests/test_v2_interview_media.py
