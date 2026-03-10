# Streamlite broker trial run
# Start the broker first in another terminal: go run ./cmd/broker

$base = "http://localhost:8080"
$dir = $PSScriptRoot

Write-Host "=== 1. Create topic 'events' (2 partitions) ===" -ForegroundColor Cyan
curl.exe -s -X POST "$base/topics" -H "Content-Type: application/json" -d "@$dir/trial-create-topic.json"
Write-Host ""

Write-Host "=== 2. Produce messages ===" -ForegroundColor Cyan
curl.exe -s -X POST "$base/produce" -H "Content-Type: application/json" -d "@$dir/trial-produce.json"
Write-Host ""
curl.exe -s -X POST "$base/produce" -H "Content-Type: application/json" -d "@$dir/trial-produce2.json"
Write-Host ""

Write-Host "=== 3. Fetch from partition 0 ===" -ForegroundColor Cyan
curl.exe -s "$base/fetch?topic=events&partition=0&offset=0"
Write-Host ""

Write-Host "=== 4. Fetch from partition 1 ===" -ForegroundColor Cyan
curl.exe -s "$base/fetch?topic=events&partition=1&offset=0"
Write-Host ""

Write-Host "Done." -ForegroundColor Green
