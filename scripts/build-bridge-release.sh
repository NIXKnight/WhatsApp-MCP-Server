#!/usr/bin/env bash
# Build the Go bridge twice and publish only when both SHA-256 digests match.
#
# A clean tree produces a deployable revision-named release. A dirty tree is
# refused unless --allow-dirty is explicit; that mode records a deterministic
# digest of tracked bridge changes plus relevant untracked bridge source and
# marks the result NONDEPLOYABLE. This demonstrates same-invocation stability,
# not reproducibility across different toolchains, hosts, or dependency caches.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/build-bridge-release.sh [--allow-dirty]

Build output: bridge/dist/releases/<release-id>/
EOF
}

allow_dirty=false
case "${1:-}" in
  "") ;;
  --allow-dirty) allow_dirty=true ;;
  -h|--help) usage; exit 0 ;;
  *) usage >&2; exit 64 ;;
esac
if (( $# > 1 )); then
  usage >&2
  exit 64
fi

script_dir="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
bridge_dir="$repo_root/bridge"
output_root="$bridge_dir/dist/releases"
base_revision="$(git -C "$repo_root" rev-parse HEAD)"
base_short="$(git -C "$repo_root" rev-parse --short=12 HEAD)"

if [[ ! -f "$bridge_dir/go.mod" || ! -f "$bridge_dir/go.sum" ]]; then
  printf '%s\n' 'Bridge module metadata is incomplete.' >&2
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  printf '%s\n' 'Go is required but was not found in PATH.' >&2
  exit 127
fi

# Every Go query and compilation uses the same explicit environment policy.
# In particular, user GOFLAGS, persistent GOENV settings, and a parent go.work
# cannot alter the effective build, and automatic toolchain download is off.
go_clean() {
  env GOFLAGS= GOENV=off GOWORK=off GOTOOLCHAIN=local CGO_ENABLED=0 go "$@"
}

go_version="$(go_clean env GOVERSION)"
if [[ ! "$go_version" =~ ^go1\.([0-9]+)(\.|$) ]] || (( BASH_REMATCH[1] < 26 )); then
  printf '%s\n' 'Go 1.26 or newer is required (automatic toolchain download is disabled).' >&2
  exit 1
fi

go_target="$(go_clean env GOOS GOARCH)"
go_os="${go_target%%$'\n'*}"
go_arch="${go_target#*$'\n'}"
if [[ -z "$go_os" || -z "$go_arch" || "$go_arch" == "$go_target" || "$go_arch" == *$'\n'* ]]; then
  printf '%s\n' 'Unable to determine the sanitized Go target.' >&2
  exit 1
fi

# Git normally hides ignored files. Refuse ignored source/module/migration
# candidates before asking Go to inspect packages so they cannot influence a
# build while being omitted from the dirty source digest.
mapfile -d '' ignored_bridge_files < <(
  git -C "$repo_root" ls-files --others --ignored --exclude-standard -z -- bridge
)
for path in "${ignored_bridge_files[@]}"; do
  case "$path" in
    bridge/*.go|bridge/migrations/*.sql|bridge/go.mod|bridge/go.sum)
      printf '%s\n' 'Refusing build: ignored build-eligible bridge input detected.' >&2
      exit 1
      ;;
  esac
done

# Discover files actually selected by go:embed. The template emits absolute
# paths one per line; Go rejects newline-bearing embed names, so line framing is
# unambiguous for a valid package. Ignore blank package separator lines.
embed_listing="$(
  cd -- "$bridge_dir"
  # shellcheck disable=SC2016 # Single quotes protect the Go template from Bash.
  go_clean list -f '{{ $dir := .Dir }}{{ range .EmbedFiles }}{{ printf "%s/%s\n" $dir . }}{{ end }}' ./...
)"
embed_sources=()
while IFS= read -r embed_path; do
  [[ -n "$embed_path" ]] || continue
  case "$embed_path" in
    "$bridge_dir"/*) path="bridge/${embed_path#"$bridge_dir"/}" ;;
    *)
      printf '%s\n' 'Refusing build: go:embed input resolved outside the bridge.' >&2
      exit 1
      ;;
  esac
  if git -C "$repo_root" check-ignore -q -- "$path"; then
    printf '%s\n' 'Refusing build: ignored go:embed input detected.' >&2
    exit 1
  else
    check_ignore_status=$?
    if (( check_ignore_status != 1 )); then
      printf '%s\n' 'Unable to verify go:embed ignore status.' >&2
      exit 1
    fi
  fi
  embed_sources+=("$path")
done <<< "$embed_listing"

# Ignore generated release output only. Any other staged, unstaged, or
# untracked repository content makes a normal release ineligible.
status_output="$(git -C "$repo_root" status --porcelain=v1 --untracked-files=normal -- . ':(exclude)bridge/dist')"
status=clean
deployable=true
source_digest="git-tree:$(git -C "$repo_root" rev-parse HEAD:bridge)"
release_id="$base_short"

if [[ -n "$status_output" ]]; then
  status=dirty
  deployable=false
  if [[ "$allow_dirty" != true ]]; then
    printf '%s\n' 'Refusing deployable release from a dirty tree; use --allow-dirty for a NONDEPLOYABLE artifact.' >&2
    exit 2
  fi

  # Hash a path-framed snapshot of every current Go/module/migration/embed
  # input and this build script. This covers tracked, staged, and untracked app
  # source, including deletions, without depending on diff formatting. The
  # allowlist excludes .env files, dist output, runtime data, and tool caches.
  mapfile -d '' digest_sources < <(
    {
      git -C "$repo_root" ls-files --cached --others --exclude-standard -z -- \
        bridge scripts/build-bridge-release.sh |
        while IFS= read -r -d '' path; do
          case "$path" in
            bridge/*.go|bridge/migrations/*.sql|bridge/go.mod|bridge/go.sum|scripts/build-bridge-release.sh)
              printf '%s\0' "$path"
              ;;
          esac
        done
      for path in "${embed_sources[@]}"; do
        printf '%s\0' "$path"
      done
    } | LC_ALL=C sort -zu
  )

  source_digest_hex="$({
    printf 'bridge-source-snapshot-v1\0'
    for path in "${digest_sources[@]}"; do
      if [[ -f "$repo_root/$path" ]]; then
        printf 'file\0%s\0' "$path"
        cat -- "$repo_root/$path"
        printf '\0'
      else
        printf 'deleted\0%s\0' "$path"
      fi
    done
  } | sha256sum | awk '{print $1}')"
  source_digest="sha256:$source_digest_hex"
  release_id="${base_short}-dirty-${source_digest_hex:0:16}"
fi

mkdir -p -- "$output_root"
tmp_dir="$(mktemp -d "$bridge_dir/dist/.bridge-build.XXXXXX")"
trap 'rm -rf -- "$tmp_dir"' EXIT

build_one() {
  local output=$1
  (
    cd -- "$bridge_dir"
    go_clean build \
      -mod=readonly \
      -trimpath \
      -ldflags='-buildid=' \
      -o "$output" \
      .
  )
}

build_one "$tmp_dir/whatsapp-bridge.1"
build_one "$tmp_dir/whatsapp-bridge.2"
hash_one="$(sha256sum "$tmp_dir/whatsapp-bridge.1" | awk '{print $1}')"
hash_two="$(sha256sum "$tmp_dir/whatsapp-bridge.2" | awk '{print $1}')"
if [[ "$hash_one" != "$hash_two" ]]; then
  printf '%s\n' 'Two-pass bridge build hash mismatch; no artifact published.' >&2
  exit 1
fi

release_dir="$output_root/$release_id"
mkdir -p -- "$release_dir"
install -m 0755 "$tmp_dir/whatsapp-bridge.1" "$release_dir/whatsapp-bridge"
printf '%s  %s\n' "$hash_one" 'whatsapp-bridge' > "$release_dir/whatsapp-bridge.sha256"
cat > "$release_dir/provenance.txt" <<EOF
status=$status
deployable=$deployable
base_revision=$base_revision
source_digest=$source_digest
go_version=$go_version
go_os=$go_os
go_arch=$go_arch
go_environment=GOFLAGS-empty GOENV-off GOWORK-off GOTOOLCHAIN-local CGO_ENABLED-0
build_flags=-mod=readonly -trimpath -ldflags=-buildid=
stability_scope=two-pass-same-invocation-only
binary_sha256=$hash_one
EOF

if [[ "$deployable" != true ]]; then
  : > "$release_dir/NONDEPLOYABLE"
else
  rm -f -- "$release_dir/NONDEPLOYABLE"
fi

printf 'release_dir=%s\n' "$release_dir"
printf 'binary_sha256=%s\n' "$hash_one"
printf 'status=%s\n' "$status"
printf 'deployable=%s\n' "$deployable"
