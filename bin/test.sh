#!/usr/bin/env bash
# Parallel Transcoder test suite.
# Runs Rust tests, shell/JS syntax checks, binary smoke tests,
# and a desktop launcher startup test. Add new tests by defining
# a test_* function and registering it in the `run` calls below.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

OS="$(uname -s)"
case "$OS" in
    Darwin) export DYLD_LIBRARY_PATH="$PROJECT_ROOT/lib:${DYLD_LIBRARY_PATH:-}" ;;
    Linux)  export LD_LIBRARY_PATH="$PROJECT_ROOT/lib:${LD_LIBRARY_PATH:-}" ;;
esac

DESKTOP_LAUNCH_WAIT="${DESKTOP_LAUNCH_WAIT:-6}"
SKIP_DESKTOP_LAUNCH="${SKIP_DESKTOP_LAUNCH:-0}"
SKIP_CARGO="${SKIP_CARGO:-0}"

PASS=0
FAIL=0
RESULTS=()

run() {
    local name="$1"; shift
    echo ""
    echo "==> $name"
    if "$@"; then
        PASS=$((PASS + 1))
        RESULTS+=("PASS  $name")
    else
        FAIL=$((FAIL + 1))
        RESULTS+=("FAIL  $name")
    fi
}

# --- Rust unit & integration tests ---
test_cargo() {
    if [[ "$SKIP_CARGO" == "1" ]]; then
        echo "  SKIP_CARGO=1 — skipping"
        return 0
    fi
    if ! command -v cargo &>/dev/null; then
        echo "  cargo not found"
        return 1
    fi
    cargo test --workspace --quiet
}

# --- Shell script syntax validation ---
test_shell_syntax() {
    local rc=0 f
    for f in bin/*; do
        [[ -f "$f" ]] || continue
        # Shell scripts identified by shebang
        if head -1 "$f" 2>/dev/null | grep -Eq '^#!.*(bash|sh)\b'; then
            echo "  bash -n $f"
            bash -n "$f" || rc=1
        fi
    done
    for f in build.sh package.sh bin/test.sh; do
        if [[ -f "$f" ]]; then
            echo "  bash -n $f"
            bash -n "$f" || rc=1
        fi
    done
    return $rc
}

# --- Node.js script syntax validation ---
test_node_syntax() {
    if ! command -v node &>/dev/null; then
        echo "  node not found"
        return 1
    fi
    local rc=0 f
    for f in web/server.js desktop/main.js desktop/preload.js; do
        if [[ -f "$f" ]]; then
            echo "  node --check $f"
            node --check "$f" || rc=1
        fi
    done
    return $rc
}

# --- Node.js test suite (desktop + web) ---
# Bare `node --test`: a quoted glob is a hard error on Node 20 and positional
# paths mean different things on 20 vs 24; default discovery is portable.
test_node_tests() {
    if ! command -v npm &>/dev/null; then
        echo "  npm not found"
        return 1
    fi
    if [[ ! -d node_modules ]]; then
        echo "  installing npm dependencies..."
        npm ci --silent || return 1
    fi
    npm test
}

# --- Binary --help smoke tests ---
test_binary_help() {
    local rc=0 bin
    for bin in transcoder-coordinator transcoder-worker transcoder-node; do
        if [[ -x "bin/$bin" ]]; then
            echo "  bin/$bin --help"
            "bin/$bin" --help >/dev/null 2>&1 || { echo "    FAILED"; rc=1; }
        else
            echo "  skip: bin/$bin not built"
        fi
    done
    return $rc
}

# --- Desktop launcher startup test ---
# Launches bin/transcoder-desktop, waits briefly, and verifies the
# process is still alive (did not crash during startup). Kills it
# afterward. Set SKIP_DESKTOP_LAUNCH=1 to skip on headless hosts.
test_desktop_launch() {
    if [[ "$SKIP_DESKTOP_LAUNCH" == "1" ]]; then
        echo "  SKIP_DESKTOP_LAUNCH=1 — skipping"
        return 0
    fi
    if [[ ! -x bin/transcoder-desktop ]]; then
        echo "  bin/transcoder-desktop missing or not executable"
        return 1
    fi
    if [[ ! -d node_modules || ! -x node_modules/.bin/electron ]]; then
        echo "  installing npm dependencies (including devDependencies)..."
        npm install --silent --include=dev || return 1
    fi

    local logfile
    logfile="$(mktemp -t transcoder-desktop-test.XXXXXX)"
    echo "  launching (log: $logfile)"

    # Start detached in its own process group so we can kill descendants.
    set -m
    bin/transcoder-desktop >"$logfile" 2>&1 &
    local pid=$!
    set +m

    sleep "$DESKTOP_LAUNCH_WAIT"

    if kill -0 "$pid" 2>/dev/null; then
        echo "  desktop alive after ${DESKTOP_LAUNCH_WAIT}s (pid=$pid) — stopping"
        kill -TERM "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
        sleep 1
        kill -KILL "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        rm -f "$logfile"
        return 0
    else
        echo "  desktop exited prematurely — log follows:"
        sed 's/^/    /' "$logfile"
        rm -f "$logfile"
        return 1
    fi
}

echo "=== Parallel Transcoder Test Suite ==="
echo "Project: $PROJECT_ROOT"
echo "Platform: $OS $(uname -m)"

run "Rust tests (cargo test --workspace)" test_cargo
run "Shell script syntax"                 test_shell_syntax
run "Node.js script syntax"               test_node_syntax
run "Node.js test suite (npm test)"        test_node_tests
run "Binary --help smoke tests"           test_binary_help
run "Desktop launcher startup"            test_desktop_launch

echo ""
echo "=== Summary ==="
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo ""
echo "Passed: $PASS    Failed: $FAIL"

exit $FAIL
