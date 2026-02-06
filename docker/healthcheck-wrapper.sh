#!/bin/bash

UUID="${HEALTHCHECKS_UUID:-}"

if [ $# -lt 1 ] || [ -z "$1" ]; then
    echo "Usage: $0 [command to run]"
    exit 1
fi

USE_HEALTHCHECKS=false
if [ -n "$UUID" ]; then
    USE_HEALTHCHECKS=true
    curl -fsS -m 10 --retry 5 -o /dev/null "https://hc-ping.com/$UUID/start"
fi

LOG_FILE=/tmp/healtchecks.io-$(echo "$@" | md5sum | head -c 20)-$(echo $RANDOM | md5sum | head -c 20).log

set -o pipefail

"$@" | tee "$LOG_FILE"
RETURN_CODE=$?
set +o pipefail

if [ "$USE_HEALTHCHECKS" = true ]; then
    OUTPUT=$(tail --bytes=100000 "$LOG_FILE")
    curl -fsS -m 10 --retry 5 --data-raw "$OUTPUT" \
        -o /dev/null "https://hc-ping.com/$UUID/$RETURN_CODE"
fi

[ "$RETURN_CODE" -eq 0 ] && rm -f "$LOG_FILE"

exit $RETURN_CODE