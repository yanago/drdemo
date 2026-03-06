#!/usr/bin/env bash
# Demo script: create job, start, monitor, pause/resume, cancel.
# Run replay service on :8080 and (optional) mock sink on :8081 first.

set -e
BASE="${BASE_URL:-http://localhost:8080}"

echo "=== Create replay job (REST destination) ==="
RESP=$(curl -s -X POST "$BASE/api/v1/replay/jobs" \
  -H "Content-Type: application/json" \
  -d '{"table_path":"security.events","destination_type":"REST","destination_config":"http://localhost:8081","speed_multiplier":5}')
echo "$RESP" | head -c 500
echo ""
JOB_ID=$(echo "$RESP" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
if [ -z "$JOB_ID" ]; then
  echo "Failed to get job id"
  exit 1
fi
echo "Job ID: $JOB_ID"

echo ""
echo "=== Start replay ==="
curl -s -X POST "$BASE/api/v1/replay/jobs/$JOB_ID/start" | head -c 200
echo ""

echo ""
echo "=== Poll status (every 3s, Ctrl+C to stop) ==="
for i in 1 2 3 4 5 6 7 8 9 10; do
  sleep 3
  echo "--- Status ---"
  curl -s "$BASE/api/v1/replay/jobs/$JOB_ID/status"
  echo ""
  echo "--- Metrics ---"
  curl -s "$BASE/api/v1/replay/jobs/$JOB_ID/metrics"
  echo ""
  STATUS=$(curl -s "$BASE/api/v1/replay/jobs/$JOB_ID/status" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
  if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "CANCELLED" ] || [ "$STATUS" = "FAILED" ]; then
    echo "Job finished with status: $STATUS"
    break
  fi
done

echo ""
echo "=== Optional: Pause then Resume ==="
# curl -s -X POST "$BASE/api/v1/replay/jobs/$JOB_ID/pause"
# sleep 2
# curl -s -X POST "$BASE/api/v1/replay/jobs/$JOB_ID/resume"

echo "Done. Check mock sink: curl http://localhost:8081/events/count"
