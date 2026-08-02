#!/bin/bash
set -euo pipefail

# Configuration
# Path to the compiled sbe-collector binary
COLLECTOR_EXE="/home/ec2-user/sbe-collector"
# Base directory for data storage
BASE_DATA_DIR="./data/sbe"
# Coins to collect
COINS=("btc" "eth" "bnb" "xrp" "sol" "trx" "doge")
# Redundant websocket connections per collector
CONNECTIONS=2

# Mappings for data paths
# Format: "internal_exchange_name:filesystem_subpath"
MAPPINGS=(
    "binancesbespot:binance/spot"
)

SESSION_NAME="sbe_collector"
# Session names this script has created in the past.
#
# Renaming the session without killing the old one leaves the previous
# collector running against the same data directory. Two processes appending to
# one symbol's file interleave their zstd frames and render it undecodable, and
# they double this IP's REST usage — which Binance answers with a ban.
LEGACY_SESSION_NAMES=("sbe_collection")

if [ ! -x "$COLLECTOR_EXE" ]; then
    echo "Error: $COLLECTOR_EXE is missing or not executable." >&2
    exit 1
fi

# Check for API Key (required by binancesbespot). Read silently: -p alone echoes
# the key into the terminal and, from there, into scrollback.
#
# `|| true` because `read` reports failure on EOF — which is what happens when
# this runs without a terminal, exactly the case the message below exists to
# explain. Under `set -e` that failure would abort here and the operator would
# see a bare prompt and exit 1 instead of the reason.
if [ -z "${BINANCE_API_KEY:-}" ]; then
    read -rsp "Enter BINANCE_API_KEY: " BINANCE_API_KEY || true
    echo
    if [ -z "$BINANCE_API_KEY" ]; then
        echo "Error: BINANCE_API_KEY is required." >&2
        exit 1
    fi
fi

# Kill the current session and any this script created under an older name, so
# a rename cannot leave two collectors writing the same files.
for NAME in "$SESSION_NAME" "${LEGACY_SESSION_NAMES[@]}"; do
    tmux kill-session -t "$NAME" 2>/dev/null || true
done

tmux new-session -d -s "$SESSION_NAME" -n "init"

# The collector reads the key from its environment, so hand it over through the
# session environment rather than the collector's command line, where it would
# be typed into the pane — recorded in that shell's history file, and readable
# in `ps` by every user on the host for as long as the collector runs.
#
# This narrows that exposure rather than removing it: the key is on *this*
# command's argv while the tmux client runs, and `tmux show-environment -t
# sbe_collector` prints it afterwards to anyone who can reach the socket. Closing
# those would mean the collector reading a mode-600 file instead of an
# environment variable.
tmux set-environment -t "$SESSION_NAME" BINANCE_API_KEY "$BINANCE_API_KEY"

for MAP in "${MAPPINGS[@]}"; do
    EXCH="${MAP%%:*}"
    SUBPATH="${MAP#*:}"
    TARGET_DIR="$BASE_DATA_DIR/$SUBPATH"

    # Ensure target directory exists
    mkdir -p "$TARGET_DIR"

    # Build the symbols list for this exchange (assuming <coin>usdt convention)
    SYMBOLS_LIST=""
    for COIN in "${COINS[@]}"; do
        S="${COIN}usdt"
        SYMBOLS_LIST+="$S "
    done

    # Create a new tmux window for this exchange instance. It inherits the
    # session environment set above, so the command below carries no secret.
    tmux new-window -t "$SESSION_NAME" -n "$EXCH"

    CMD="$COLLECTOR_EXE -c $CONNECTIONS $TARGET_DIR $EXCH $SYMBOLS_LIST"

    # Send the command to the tmux window
    tmux send-keys -t "$SESSION_NAME:$EXCH" "$CMD" C-m
done

# Cleanup the initial window. The collectors are live by now, so a tidy-up that
# fails — a stale server, a window already gone — must not turn a successful
# deployment into a non-zero exit under `set -e`.
tmux kill-window -t "$SESSION_NAME:init" || true

echo "SBE Collection started in tmux session: $SESSION_NAME"
echo "Attach with: tmux attach-session -t $SESSION_NAME"

# Automatically attach, but only when there is somewhere to attach to.
#
# The collectors are already running by this point, so this is a convenience and
# never a failure. Without the guard, `set -e` turns a refused attach into a
# non-zero exit — so cron, systemd or `ssh host ./run_sbe.sh` records a
# successful deployment as failed, and a retry would kill the session it just
# started. `$TMUX` is checked too: run from inside a tmux pane, which is how an
# operator on a remote host usually runs this, both stdin and stdout are
# terminals but tmux still refuses to nest and exits non-zero.
if [ -z "${TMUX:-}" ] && [ -t 0 ] && [ -t 1 ]; then
    tmux attach-session -t "$SESSION_NAME"
fi
