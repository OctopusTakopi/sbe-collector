#!/bin/bash

# Configuration
# Path to the compiled sbe-collector binary
COLLECTOR_EXE="/home/ec2-user/sbe-collector"
# Base directory for data storage
BASE_DATA_DIR="./data/sbe"
# Coins to collect
COINS=("btc" "eth" "sol")

# Mappings for data paths
# Format: "internal_exchange_name:filesystem_subpath"
MAPPINGS=(
    "binancesbespot:binance/spot"
)

SESSION_NAME="sbe_collection"

# Check for API Key (required by binancesbespot)
if [ -z "$BINANCE_API_KEY" ]; then
    read -r -p "Enter BINANCE_API_KEY: " BINANCE_API_KEY
    if [ -z "$BINANCE_API_KEY" ]; then
        echo "Error: BINANCE_API_KEY is required."
        exit 1
    fi
    export BINANCE_API_KEY
fi

# Kill existing session to start fresh
tmux kill-session -t $SESSION_NAME 2>/dev/null
tmux new-session -d -s $SESSION_NAME -n "init"

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

    # Create a new tmux window for this exchange instance
    tmux new-window -t $SESSION_NAME -n "$EXCH"
    
    # Construct the command - pass API key explicitly to the process
    CMD="BINANCE_API_KEY=$BINANCE_API_KEY $COLLECTOR_EXE $TARGET_DIR $EXCH $SYMBOLS_LIST"
    
    # Send the command to the tmux window
    tmux send-keys -t "$SESSION_NAME:$EXCH" "$CMD" C-m
done

# Cleanup the initial window
tmux kill-window -t "$SESSION_NAME:init"

echo "SBE Collection started in tmux session: $SESSION_NAME"
echo "Attach with: tmux attach-session -t $SESSION_NAME"

# Automatically attach
tmux attach-session -t $SESSION_NAME
