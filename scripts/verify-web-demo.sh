#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
readonly SITE_DIR="$REPO_ROOT/website/public"
readonly DEMO_HTML="$SITE_DIR/demo/index.html"
readonly ASSET_DIR="$SITE_DIR/demo/assets"
readonly MANIFEST="$ASSET_DIR/manifest.json"
readonly DEMO_LOADER="$SITE_DIR/js/demo-loader.js"

fail() {
    echo "web artifact verification: $*" >&2
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

for command_name in awk basename find grep jq strings; do
    require_command "$command_name"
done

[[ ! -L "$SITE_DIR" ]] || fail "refusing symlinked site directory"
[[ -f "$DEMO_HTML" ]] || fail "missing demo page: $DEMO_HTML"
[[ -f "$MANIFEST" ]] || fail "missing web asset manifest: $MANIFEST"
[[ -f "$DEMO_LOADER" ]] || fail "missing demo loader: $DEMO_LOADER"
jq -e 'type == "object" and (keys == ["display_version_suffix", "js", "js_sha256", "source_commit", "source_dirty", "wasm", "wasm_sha256"])' "$MANIFEST" >/dev/null \
    || fail "manifest has unexpected fields"

js_path="$(jq -er '.js | select(type == "string" and test("^/demo/assets/perspecta_web-[0-9a-f]{16}\\.js$"))' "$MANIFEST")" \
    || fail "manifest has no valid .js path"
wasm_path="$(jq -er '.wasm | select(type == "string" and test("^/demo/assets/perspecta_web_bg-[0-9a-f]{16}\\.wasm$"))' "$MANIFEST")" \
    || fail "manifest has no valid .wasm path"
js_sha256="$(jq -er '.js_sha256 | select(test("^[0-9a-f]{64}$"))' "$MANIFEST")" \
    || fail "manifest has no valid JavaScript digest"
wasm_sha256="$(jq -er '.wasm_sha256 | select(test("^[0-9a-f]{64}$"))' "$MANIFEST")" \
    || fail "manifest has no valid WASM digest"
source_commit="$(jq -er '.source_commit | select(type == "string" and test("^[0-9a-f]{40,64}$"))' "$MANIFEST")" \
    || fail "manifest has no valid source commit"
source_dirty="$(jq -er '.source_dirty | select(type == "boolean") | tostring' "$MANIFEST")" \
    || fail "manifest has no valid source dirty state"
display_version_suffix="$(jq -er '.display_version_suffix | select(type == "string" and test("^\\+web\\.[0-9a-f]{7}(\\.dirty)?$"))' "$MANIFEST")" \
    || fail "manifest has no valid display version suffix"

expected_display_version_suffix="+web.${source_commit:0:7}"
if [[ "$source_dirty" == "true" ]]; then
    expected_display_version_suffix="$expected_display_version_suffix.dirty"
fi
if [[ "$display_version_suffix" != "$expected_display_version_suffix" ]]; then
    fail "manifest display version suffix does not match its source metadata"
fi

if [[ -n "${PERSPECTA_WEB_SOURCE_COMMIT:-}" && "$source_commit" != "$PERSPECTA_WEB_SOURCE_COMMIT" ]]; then
    fail "manifest source commit does not match the expected Pages commit"
fi
if [[ "${GITHUB_ACTIONS:-}" == "true" && "$source_dirty" != "false" ]]; then
    fail "Pages source metadata unexpectedly marks the checkout dirty"
fi

readonly JS_FILE="$SITE_DIR$js_path"
readonly WASM_FILE="$SITE_DIR$wasm_path"
[[ -f "$JS_FILE" ]] || fail "manifest JavaScript is missing: $js_path"
[[ -f "$WASM_FILE" ]] || fail "manifest WASM is missing: $wasm_path"

[[ "$(sha256_file "$JS_FILE")" == "$js_sha256" ]] \
    || fail "JavaScript digest does not match manifest"
[[ "$(sha256_file "$WASM_FILE")" == "$wasm_sha256" ]] \
    || fail "WASM digest does not match manifest"
[[ "$(basename -- "$js_path")" == "perspecta_web-${js_sha256:0:16}.js" ]] \
    || fail "JavaScript filename does not match its digest"
[[ "$(basename -- "$wasm_path")" == "perspecta_web_bg-${wasm_sha256:0:16}.wasm" ]] \
    || fail "WASM filename does not match its digest"

if find "$ASSET_DIR" -maxdepth 1 -type f \( -name '*.js' -o -name '*.wasm' \) \
    ! -path "$JS_FILE" ! -path "$WASM_FILE" -print -quit | grep -E '.' >/dev/null; then
    fail "unhashed, stale, or unexpected executable web assets were published"
fi

grep -Fq "$js_path" "$DEMO_HTML" \
    || fail "demo HTML does not reference the manifest JavaScript"
grep -Fq "$(basename -- "$wasm_path")" "$JS_FILE" \
    || fail "generated JavaScript does not reference the hashed WASM file"
grep -Fq 'queue_dropped_files' "$JS_FILE" \
    || fail "generated JavaScript does not export the atomic file-drop bridge"
grep -Fq 'queue_dropped_files' "$DEMO_LOADER" \
    || fail "demo loader does not use the atomic file-drop bridge"
grep -Fq 'Web preview' "$DEMO_HTML" \
    || fail "demo footer does not identify the web preview"
grep -Eq "data-preview-sha=(\"$source_commit\"|$source_commit)([[:space:]>])" "$DEMO_HTML" \
    || fail "demo footer does not expose the full source commit"
grep -Eq "data-preview-dirty=(\"$source_dirty\"|$source_dirty)([[:space:]>])" "$DEMO_HTML" \
    || fail "demo footer does not expose the source dirty state"
grep -Fq "${source_commit:0:7}</code>" "$DEMO_HTML" \
    || fail "demo footer does not show the short source commit"
grep -Fq "https://github.com/timcogan/perspecta/commit/$source_commit" "$DEMO_HTML" \
    || fail "demo footer does not link to the full source commit"

if find "$SITE_DIR" -type f \( -name '*.map' -o -name '*.d.ts' \) -print -quit \
    | grep -E '.' >/dev/null; then
    fail "source map or declaration artifacts were published"
fi
if grep -RIl --include='*.js' --include='*.html' 'sourceMappingURL=' "$SITE_DIR" \
    | grep -E '.' >/dev/null; then
    fail "a published JavaScript or HTML file references a source map"
fi

for local_prefix in "$REPO_ROOT" "${HOME:-}" "${CARGO_HOME:-}"; do
    if [[ -n "$local_prefix" ]] \
        && LC_ALL=C grep -RIlF --binary-files=text -- "$local_prefix" "$SITE_DIR" >/dev/null; then
        fail "published output contains a build-host filesystem path"
    fi
done
if LC_ALL=C grep -RIlE --binary-files=text '(/home/|/Users/|/root/|/private/var/folders/|/build-home/|/build-users/|[A-Za-z]:\\Users\\)' "$SITE_DIR" >/dev/null; then
    fail "published output contains a local filesystem path"
fi
if strings "$WASM_FILE" | grep -E '\.debug_(abbrev|info|line|str)([^[:alnum:]_]|$)' >/dev/null; then
    fail "published WASM contains a DWARF debug section"
fi
if strings "$WASM_FILE" | grep -F 'egui_memory_ron' >/dev/null; then
    fail "published WASM enables egui local-storage persistence"
fi

package_version="$(awk '
    /^\[package\]$/ { in_package = 1; next }
    /^\[/ { in_package = 0 }
    in_package && /^version = / {
        gsub(/"/, "", $3)
        print $3
        exit
    }
' "$REPO_ROOT/Cargo.toml")"
[[ -n "$package_version" ]] || fail "could not determine the package version"
if ! strings "$WASM_FILE" | grep -F "$package_version$display_version_suffix" >/dev/null; then
    fail "published WASM does not contain its source-qualified display version"
fi

readonly REQUIRED_CSP="default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self' 'sha256-9tSqG2Th9gNn6sbn1ZYMSqakJLznxnsamRDeigez2Mo='; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'self'; form-action 'none'; worker-src 'none'"
grep -Eq 'http-equiv=("Content-Security-Policy"|Content-Security-Policy)' "$DEMO_HTML" \
    || fail "demo HTML has no Content-Security-Policy meta element"
grep -Fq "$REQUIRED_CSP" "$DEMO_HTML" \
    || fail "demo HTML is missing the required restrictive CSP"
grep -Fq '/js/demo-loader.js' "$DEMO_HTML" \
    || fail "demo HTML does not reference the same-origin demo loader"

if grep -Eiq 'consent\.js|cookie-banner|document\.cookie|localStorage|sessionStorage|indexedDB' "$DEMO_HTML" "$DEMO_LOADER"; then
    fail "demo shell contains consent, cookie, or browser-persistence code"
fi

if grep -Eiq 'googletagmanager|google-analytics|gtag\(|dataLayer|G-[A-Z0-9]{10}([^A-Z0-9]|$)' "$DEMO_HTML" "$DEMO_LOADER"; then
    fail "demo HTML contains Google Analytics bootstrap"
fi
if grep -Eiq "<(script|img|iframe|source)[^>]+src=[\"']?//" "$DEMO_HTML"; then
    fail "demo HTML loads a protocol-relative executable or media resource"
fi
if grep -Eio '<(script|img|iframe|source)[^>]+src=[^>]*https?://[^>]*>' "$DEMO_HTML" \
    | grep -Eiv 'https://perspecta\.cogan\.dev/' \
    | grep -E '.' >/dev/null; then
    fail "demo HTML loads an executable or media resource from a third-party origin"
fi
if {
    grep -Eio '<link[^>]+(stylesheet|preload|modulepreload|preconnect)[^>]+https?://[^>]*>' "$DEMO_HTML" || true
    grep -Eio '<link[^>]+https?://[^>]+(stylesheet|preload|modulepreload|preconnect)[^>]*>' "$DEMO_HTML" || true
} | grep -Eiv 'https://perspecta\.cogan\.dev/' | grep -E '.' >/dev/null; then
    fail "demo HTML loads a linked resource from a third-party origin"
fi

echo "web artifact verification: passed"
