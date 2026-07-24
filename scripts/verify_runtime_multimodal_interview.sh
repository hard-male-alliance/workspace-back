#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPOSITORY_ROOT"

IMAGE="${AIWS_VERIFY_IMAGE:-ai-job-workspace-backend:multimodal-verification}"
ARTIFACT_DIRECTORY="${AIWS_VERIFY_DIRECTORY:-artifacts/multimodal}"
WAVE_INPUT="${AIWS_VERIFY_AUDIO_WAV:-$ARTIFACT_DIRECTORY/candidate.wav}"
AUDIO_INPUT="$ARTIFACT_DIRECTORY/candidate.ogg"
VIDEO_INPUT="$ARTIFACT_DIRECTORY/system-design.mp4"
KEYFRAME_INPUT="$ARTIFACT_DIRECTORY/system-design.jpg"
OUTPUT="$ARTIFACT_DIRECTORY/result.json"
CONFIG_COPY_DIRECTORY="$(mktemp -d)"
CONFIG_COPY="$CONFIG_COPY_DIRECTORY/config.jsonc"
install -m 0644 config.jsonc "$CONFIG_COPY"
trap 'rm -rf "$CONFIG_COPY_DIRECTORY"' EXIT

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

docker_path() {
  if "$windows_docker"; then
    wslpath -w "$1"
  else
    printf '%s\n' "$1"
  fi
}

mkdir -p "$ARTIFACT_DIRECTORY"
if [[ ! -s "$WAVE_INPUT" ]]; then
  if command -v powershell.exe >/dev/null 2>&1; then
    wave_windows="$(wslpath -w "$WAVE_INPUT")"
    powershell.exe -NoProfile -NonInteractive -Command \
      "\$ErrorActionPreference='Stop'; Add-Type -AssemblyName System.Speech; "\
"\$s=New-Object System.Speech.Synthesis.SpeechSynthesizer; "\
"\$s.SelectVoice('Microsoft Huihui Desktop'); "\
"\$s.SetOutputToWaveFile('$wave_windows'); "\
"\$s.Speak('在我负责的异步任务平台中，我使用数据库事务同时写入业务状态和 outbox 事件。worker 使用租约和幂等键处理重复投递，并通过指标监控队列延迟和失败重试。这样即使进程崩溃，任务也可以安全恢复。'); "\
"\$s.Dispose()"
  else
    echo "Provide a spoken WAV file through AIWS_VERIFY_AUDIO_WAV." >&2
    exit 1
  fi
fi

build_context="$REPOSITORY_ROOT"
if "$windows_docker"; then
  build_context="$(wslpath -w "$REPOSITORY_ROOT")"
fi
"${docker_command[@]}" build --tag "$IMAGE" "$build_context"

artifact_mount="$(docker_path "$REPOSITORY_ROOT/$ARTIFACT_DIRECTORY")"
"${docker_command[@]}" run --rm --user 0:0 \
  --entrypoint ffmpeg \
  --volume "$artifact_mount:/work" \
  "$IMAGE" \
  -nostdin -y -v error \
  -i "/work/$(basename "$WAVE_INPUT")" \
  -c:a libopus -b:a 64k \
  "/work/$(basename "$AUDIO_INPUT")"

"${docker_command[@]}" run --rm --user 0:0 \
  --entrypoint ffmpeg \
  --volume "$artifact_mount:/work" \
  "$IMAGE" \
  -nostdin -y -v error \
  -f lavfi -i "color=c=0xF4F7FB:s=1280x720:d=12:r=24" \
  -vf "drawbox=x=60:y=70:w=1160:h=580:color=0x17324D:t=5,\
drawtext=fontfile=/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc:text='RELIABLE ASYNC JOB SYSTEM':fontcolor=0x17324D:fontsize=42:x=330:y=95,\
drawbox=x=120:y=230:w=220:h=100:color=0x5B8FF9:t=fill,\
drawtext=fontfile=/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc:text='API + DB TX':fontcolor=white:fontsize=28:x=145:y=263,\
drawbox=x=530:y=230:w=220:h=100:color=0x61DDAA:t=fill,\
drawtext=fontfile=/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc:text='OUTBOX':fontcolor=0x17324D:fontsize=30:x=580:y=263,\
drawbox=x=940:y=230:w=220:h=100:color=0x65789B:t=fill,\
drawtext=fontfile=/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc:text='WORKER':fontcolor=white:fontsize=30:x=985:y=263,\
drawbox=x=350:y=455:w=260:h=100:color=0xF6BD16:t=fill,\
drawtext=fontfile=/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc:text='LEASE + RETRY':fontcolor=0x17324D:fontsize=27:x=375:y=488,\
drawbox=x=700:y=455:w=260:h=100:color=0xE8684A:t=fill,\
drawtext=fontfile=/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc:text='IDEMPOTENCY':fontcolor=white:fontsize=27:x=735:y=488" \
  -c:v libx264 -pix_fmt yuv420p \
  "/work/$(basename "$VIDEO_INPUT")"

"${docker_command[@]}" run --rm --user 0:0 \
  --entrypoint ffmpeg \
  --volume "$artifact_mount:/work" \
  "$IMAGE" \
  -nostdin -y -v error \
  -ss 1 -i "/work/$(basename "$VIDEO_INPUT")" -frames:v 1 \
  "/work/$(basename "$KEYFRAME_INPUT")"

container_id="$(
  "${docker_command[@]}" create \
    --entrypoint python \
    "$IMAGE" \
    /tmp/verify_multimodal_interview.py \
    --config /tmp/config.jsonc \
    --audio /tmp/candidate.ogg \
    --video /tmp/system-design.mp4 \
    --keyframe /tmp/system-design.jpg \
    --output /tmp/result.json \
    --environment production
)"
cleanup() {
  "${docker_command[@]}" rm -f "$container_id" >/dev/null 2>&1 || true
  rm -rf "$CONFIG_COPY_DIRECTORY"
}
trap cleanup EXIT

"${docker_command[@]}" cp \
  "$(docker_path "$REPOSITORY_ROOT/scripts/verify_multimodal_interview.py")" \
  "$container_id":/tmp/verify_multimodal_interview.py
"${docker_command[@]}" cp \
  "$(docker_path "$CONFIG_COPY")" \
  "$container_id":/tmp/config.jsonc
"${docker_command[@]}" cp \
  "$(docker_path "$REPOSITORY_ROOT/$AUDIO_INPUT")" \
  "$container_id":/tmp/candidate.ogg
"${docker_command[@]}" cp \
  "$(docker_path "$REPOSITORY_ROOT/$VIDEO_INPUT")" \
  "$container_id":/tmp/system-design.mp4
"${docker_command[@]}" cp \
  "$(docker_path "$REPOSITORY_ROOT/$KEYFRAME_INPUT")" \
  "$container_id":/tmp/system-design.jpg
"${docker_command[@]}" start -a "$container_id"
"${docker_command[@]}" cp \
  "$container_id":/tmp/result.json \
  "$(docker_path "$REPOSITORY_ROOT/$OUTPUT")"

printf 'Multimodal verification result: %s\n' "$REPOSITORY_ROOT/$OUTPUT"
