#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
readonly ASSET_DIR="$REPO_ROOT/website/static/demo/assets"
readonly DATA_MANIFEST="$REPO_ROOT/website/data/demo_assets.json"
readonly WASM_TARGET="wasm32-unknown-unknown"
readonly OUT_NAME="perspecta_web"

fail() {
    echo "web asset build: $*" >&2
    exit 1
}

if (( $# != 0 )); then
    fail "this script does not accept arguments"
fi

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command '$1' is not installed"
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{ print $1 }'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{ print $1 }'
    else
        fail "required command 'sha256sum' or 'shasum' is not installed"
    fi
}

for command_name in awk cargo find git grep install jq mktemp rustup sed wasm-bindgen; do
    require_command "$command_name"
done

[[ ! -L "$ASSET_DIR" ]] || fail "refusing symlinked asset directory"
[[ ! -L "$DATA_MANIFEST" ]] || fail "refusing symlinked data manifest"

actual_commit="$(git -C "$REPO_ROOT" rev-parse --verify 'HEAD^{commit}' 2>/dev/null)" \
    || fail "could not resolve the checked-out Git commit"
source_commit="${PERSPECTA_WEB_SOURCE_COMMIT:-${GITHUB_SHA:-$actual_commit}}"
if [[ ! "$source_commit" =~ ^[0-9a-f]{40,64}$ ]]; then
    fail "source commit must be a full lowercase Git object ID"
fi
if [[ "$source_commit" != "$actual_commit" ]]; then
    fail "source commit does not match the checked-out Git commit"
fi

source_dirty=false
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal)" ]]; then
    source_dirty=true
fi
readonly source_commit source_dirty

web_version_suffix="+web.${source_commit:0:7}"
if [[ "$source_dirty" == "true" ]]; then
    web_version_suffix="$web_version_suffix.dirty"
fi
if [[ -n "${PERSPECTA_VERSION_SUFFIX:-}" && "$PERSPECTA_VERSION_SUFFIX" != "$web_version_suffix" ]]; then
    fail "web display version suffix must match the checked-out source"
fi
export PERSPECTA_VERSION_SUFFIX="$web_version_suffix"
readonly web_version_suffix

if ! rustup target list --installed | grep -Fx "$WASM_TARGET" >/dev/null; then
    fail "Rust target '$WASM_TARGET' is missing; run 'rustup target add $WASM_TARGET'"
fi

locked_bindgen_version="$({ awk '
    /^\[\[package\]\]$/ { in_package = 0 }
    /^name = "wasm-bindgen"$/ { in_package = 1; next }
    in_package && /^version = / {
        gsub(/"/, "", $3)
        print $3
        exit
    }
' "$REPO_ROOT/Cargo.lock"; } || true)"
[[ -n "$locked_bindgen_version" ]] || fail "could not determine wasm-bindgen version from Cargo.lock"

installed_bindgen_version="$(wasm-bindgen --version | awk '{ print $2 }')"
if [[ "$installed_bindgen_version" != "$locked_bindgen_version" ]]; then
    fail "wasm-bindgen CLI is $installed_bindgen_version but Cargo.lock requires $locked_bindgen_version; install it with 'cargo install wasm-bindgen-cli --version $locked_bindgen_version --locked'"
fi

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/perspecta-web.XXXXXX")"
cleanup() {
    rm -rf -- "$temp_dir"
}
trap cleanup EXIT

cd -- "$REPO_ROOT"
user_home_path="${HOME:?HOME is not set}"
cargo_home_path="${CARGO_HOME:-$user_home_path/.cargo}"
case "$cargo_home_path" in
    /*) ;;
    *) cargo_home_path="$REPO_ROOT/$cargo_home_path" ;;
esac
readonly user_home_path
readonly cargo_home_path
readonly RUSTFLAG_SEPARATOR=$'\x1f'
readonly REMAP_USER_FLAG="--remap-path-prefix=$user_home_path=/build-user"
readonly REMAP_CARGO_FLAG="--remap-path-prefix=$cargo_home_path=/cargo-home"
readonly REMAP_REPO_FLAG="--remap-path-prefix=$REPO_ROOT=."
encoded_rustflags="${CARGO_ENCODED_RUSTFLAGS:-}"
for remap_flag in "$REMAP_USER_FLAG" "$REMAP_CARGO_FLAG" "$REMAP_REPO_FLAG"; do
    if [[ -n "$encoded_rustflags" ]]; then
        encoded_rustflags+="$RUSTFLAG_SEPARATOR"
    fi
    encoded_rustflags+="$remap_flag"
done
export CARGO_ENCODED_RUSTFLAGS="$encoded_rustflags"
export CARGO_PROFILE_RELEASE_LTO=true
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
# The native default enables openjp2. That crate links a standalone WASM module
# with unresolved libc allocation symbols, so JPEG 2000 remains desktop-only.
cargo build --locked --release --lib --target "$WASM_TARGET" --no-default-features

readonly INPUT_WASM="$REPO_ROOT/target/$WASM_TARGET/release/perspecta.wasm"
[[ -f "$INPUT_WASM" ]] || fail "expected Cargo output is missing: target/$WASM_TARGET/release/perspecta.wasm"

readonly BINDGEN_DIR="$temp_dir/bindgen"
mkdir -p -- "$BINDGEN_DIR"
wasm-bindgen \
    --target web \
    --no-typescript \
    --remove-name-section \
    --remove-producers-section \
    --out-name "$OUT_NAME" \
    --out-dir "$BINDGEN_DIR" \
    "$INPUT_WASM"

readonly BINDGEN_JS="$BINDGEN_DIR/$OUT_NAME.js"
readonly BINDGEN_WASM="$BINDGEN_DIR/${OUT_NAME}_bg.wasm"
[[ -f "$BINDGEN_JS" ]] || fail "wasm-bindgen did not emit $OUT_NAME.js"
[[ -f "$BINDGEN_WASM" ]] || fail "wasm-bindgen did not emit ${OUT_NAME}_bg.wasm"

wasm_sha256="$(sha256_file "$BINDGEN_WASM")"
wasm_filename="${OUT_NAME}_bg-${wasm_sha256:0:16}.wasm"

readonly PATCHED_JS="$temp_dir/$OUT_NAME.js"
sed "s/${OUT_NAME}_bg\.wasm/$wasm_filename/g" "$BINDGEN_JS" >"$PATCHED_JS"
grep -Fq "$wasm_filename" "$PATCHED_JS" || fail "could not patch the wasm-bindgen module URL"
if grep -Fq "${OUT_NAME}_bg.wasm" "$PATCHED_JS"; then
    fail "unhashed WASM module URL remains in generated JavaScript"
fi

js_sha256="$(sha256_file "$PATCHED_JS")"
js_filename="${OUT_NAME}-${js_sha256:0:16}.js"

mkdir -p -- "$ASSET_DIR" "$(dirname -- "$DATA_MANIFEST")"
find "$ASSET_DIR" -maxdepth 1 -type f \
    \( -name "${OUT_NAME}-*.js" -o -name "${OUT_NAME}_bg-*.wasm" -o -name manifest.json \) \
    -delete
install -m 0644 "$PATCHED_JS" "$ASSET_DIR/$js_filename"
install -m 0644 "$BINDGEN_WASM" "$ASSET_DIR/$wasm_filename"

readonly TEMP_MANIFEST="$temp_dir/demo_assets.json"
jq -n \
    --arg js "/demo/assets/$js_filename" \
    --arg wasm "/demo/assets/$wasm_filename" \
    --arg js_sha256 "$js_sha256" \
    --arg wasm_sha256 "$wasm_sha256" \
    --arg source_commit "$source_commit" \
    --argjson source_dirty "$source_dirty" \
    --arg display_version_suffix "$web_version_suffix" \
    '{
        js: $js,
        wasm: $wasm,
        js_sha256: $js_sha256,
        wasm_sha256: $wasm_sha256,
        source_commit: $source_commit,
        source_dirty: $source_dirty,
        display_version_suffix: $display_version_suffix
    }' >"$TEMP_MANIFEST"
install -m 0644 "$TEMP_MANIFEST" "$DATA_MANIFEST"
install -m 0644 "$TEMP_MANIFEST" "$ASSET_DIR/manifest.json"

echo "web asset build: wrote $js_filename and $wasm_filename"
