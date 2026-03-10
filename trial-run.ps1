# Streamlite broker trial run
# Start the broker first in another terminal: go run ./cmd/broker

$base = "http://localhost:8080"
$dir = $PSScriptRoot

Write-Host "=== 1. Create topic 'events' (2 partitions) ===" -ForegroundColor Cyan
curl.exe -s -X POST "$base/topics" -H "Content-Type: application/json" -d "@$dir/trial-create-topic.json"
Write-Host ""

Write-Host "=== 2. Produce messages ===" -ForegroundColor Cyan
$producesFile = Join-Path $dir "trial-produces.json"
if (Test-Path $producesFile) {
    # Single file with JSON array: send one request per element
    $messages = Get-Content $producesFile -Raw | ConvertFrom-Json
    $tempBody = Join-Path $dir ".trial-produce-body.json"
    foreach ($i in 0..($messages.Count - 1)) {
        $messages[$i] | ConvertTo-Json -Compress | Set-Content $tempBody -NoNewline
        Write-Host "  [$($i + 1)/$($messages.Count)] $($messages[$i].key): $($messages[$i].value)" -ForegroundColor Gray
        curl.exe -s -X POST "$base/produce" -H "Content-Type: application/json" -d "@$tempBody"
        Write-Host ""
    }
    Remove-Item $tempBody -ErrorAction SilentlyContinue
} else {
    # Fallback: one message per trial-produce*.json file
    Get-ChildItem -Path $dir -Filter "trial-produce*.json" | Sort-Object Name | ForEach-Object {
        Write-Host "  $($_.Name)" -ForegroundColor Gray
        curl.exe -s -X POST "$base/produce" -H "Content-Type: application/json" -d "@$($_.FullName)"
        Write-Host ""
    }
}

$eventsDir = Join-Path $dir "data\events"
if (-not (Test-Path $eventsDir)) { New-Item -ItemType Directory -Path $eventsDir -Force | Out-Null }

Write-Host "=== 3. Fetch from partition 0 ===" -ForegroundColor Cyan
try {
    $r0 = Invoke-WebRequest -Uri "$base/fetch?topic=events&partition=0&offset=0" -UseBasicParsing
    $fetch0 = $r0.Content
} catch {
    $fetch0 = "error: $_"
}
Write-Host $fetch0
$log0 = Join-Path $eventsDir "fetch-partition-0.log"
Add-Content -Path $log0 -Value ("# fetch " + (Get-Date -Format "o")) -Encoding utf8
Add-Content -Path $log0 -Value $fetch0 -Encoding utf8
Write-Host "  (appended to data/events/fetch-partition-0.log)" -ForegroundColor Gray
Write-Host ""

Write-Host "=== 4. Fetch from partition 1 ===" -ForegroundColor Cyan
try {
    $r1 = Invoke-WebRequest -Uri "$base/fetch?topic=events&partition=1&offset=0" -UseBasicParsing
    $fetch1 = $r1.Content
} catch {
    $fetch1 = "error: $_"
}
Write-Host $fetch1
$log1 = Join-Path $eventsDir "fetch-partition-1.log"
Add-Content -Path $log1 -Value ("# fetch " + (Get-Date -Format "o")) -Encoding utf8
Add-Content -Path $log1 -Value $fetch1 -Encoding utf8
Write-Host "  (appended to data/events/fetch-partition-1.log)" -ForegroundColor Gray
Write-Host ""

Write-Host "Done." -ForegroundColor Green
