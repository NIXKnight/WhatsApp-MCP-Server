#!/bin/sh
set -eu

child=0
pending_signal=''
interrupted=0
child_status=''
retained_signal_status=''
# shellcheck disable=SC2317 # Invoked indirectly by the TERM/INT traps below.
forward() {
  signal=$1
  if [ "$child" -eq 0 ]; then
    pending_signal=$signal
    return 0
  fi
  interrupted=1
  kill "-$signal" "$child" 2>/dev/null || true
}
trap 'forward TERM' TERM
trap 'forward INT' INT

"$@" &
child=$!
if [ -n "$pending_signal" ]; then
  kill "-$pending_signal" "$child" 2>/dev/null || true
  pending_signal=''
fi
while [ -z "$child_status" ]; do
  interrupted=0
  if wait "$child"; then
    observed_status=0
  else
    observed_status=$?
  fi
  if [ "$interrupted" -eq 1 ] && [ "$observed_status" -ge 128 ]; then
    retained_signal_status=$observed_status
    continue
  fi
  if [ "$observed_status" -eq 127 ] && [ -n "$retained_signal_status" ]; then
    child_status=$retained_signal_status
  else
    child_status=$observed_status
  fi
done
trap - TERM INT
if [ "$child_status" -eq 78 ] && [ "${BRIDGE_MANAGED-}" = "true" ]; then
  exit 0
fi
exit "$child_status"
