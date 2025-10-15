#!/usr/bin/env pwsh
<#
Deploy a prebuilt ble-gw-auto-parser image to Cloud Run, wiring:
- Secrets via Secret Manager (--set-secrets)
- Plain env vars from .env (--set-env-vars)
#>

[CmdletBinding()]
param(
  [switch]$SkipSecrets
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-ToolInstalled {
  param([Parameter(Mandatory)][string]$Name)
  $cmd = Get-Command $Name -ErrorAction SilentlyContinue
  if (-not $cmd) { throw "Required tool '$Name' not found in PATH." }
  return $cmd.Source
}
function Invoke-Cmd {
  param([Parameter(Mandatory)][string]$ExePath,[Parameter(Mandatory)][string[]]$Arguments)
  Write-Host "→ $ExePath $($Arguments -join ' ')" -ForegroundColor Cyan
  & $ExePath @Arguments
  if ($LASTEXITCODE -ne 0) { throw "Command failed ($LASTEXITCODE): $ExePath $($Arguments -join ' ')" }
}
function Import-DotEnv {
  param([string]$Path = ".env")
  if (-not (Test-Path -LiteralPath $Path)) { return }
  Get-Content -LiteralPath $Path | ForEach-Object {
    if ($_ -match '^\s*#') { return }
    if ($_ -match '^\s*$') { return }
    $kv = $_ -split '=', 2
    if ($kv.Count -eq 2) {
      $k = $kv[0].Trim()
      $v = $kv[1].Trim()
      if ($k) { Set-Variable -Name $k -Value $v -Scope Script -Force }
    }
  }
}

# 0) Preconditions
$Gcloud = Test-ToolInstalled -Name "gcloud"

# 1) Load .env
Import-DotEnv

# 2) Inputs (from .env)
if (-not $PROJECT_ID) { throw "PROJECT_ID missing in .env" }
if (-not $REGION)     { throw "REGION missing in .env" }
if (-not $SERVICE)    { $SERVICE = "ble-gw-auto-parser" }

# Prebuilt image reference (full path)
if (-not $IMAGE) {
  if (-not $REPO -or -not $IMAGE_TAG) { throw "IMAGE is missing in .env (or provide REPO and IMAGE_TAG)" }
  $IMAGE = "$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/${SERVICE}:$IMAGE_TAG"
}

$saEmail = "$($SERVICE)@$PROJECT_ID.iam.gserviceaccount.com"

Write-Host "`n=== Deploy config ===" -ForegroundColor Yellow
Write-Host "Project:       $PROJECT_ID"
Write-Host "Region:        $REGION"
Write-Host "Service:       $SERVICE"
Write-Host "Image:         $IMAGE"
Write-Host "ServiceAcct:   $saEmail"
Write-Host "Env PROJECT_ID:        $PROJECT_ID"
Write-Host "Env PUBSUB_TOPIC_GW_SELF: $PUBSUB_TOPIC_GW_SELF"
Write-Host "SkipSecrets:   $SkipSecrets"
Write-Host ""

# 3) Ensure project set
Invoke-Cmd $Gcloud @("config","set","project",$PROJECT_ID)

# 4) Ensure service account exists
$saExists = $false
$saList = & $Gcloud iam service-accounts list --format="value(email)" --filter="email:$saEmail" 2>$null
if ($LASTEXITCODE -eq 0 -and $saList -match [Regex]::Escape($saEmail)) { $saExists = $true }
if (-not $saExists) {
  Invoke-Cmd $Gcloud @("iam","service-accounts","create",$SERVICE,"--display-name","SA for $SERVICE","--quiet")
}

# 5) Grant IAM: Cloud SQL Client
Invoke-Cmd $Gcloud @("projects","add-iam-policy-binding",$PROJECT_ID,"--member","serviceAccount:$saEmail","--role","roles/cloudsql.client")

# 6) Build secrets list dynamically (include only those that exist)
$requiredSecrets = @("DB_USER","DB_PASSWORD","DB_NAME","INSTANCE_CONNECTION_NAME")

# Optional envs that might be provided via Secret Manager; if present, prefer secret over literal
$optionalSecretEnvVars = @("PROJECT_ID","PUBSUB_TOPIC_GW_SELF")

$pairs = @()      # for --set-secrets
foreach ($name in $requiredSecrets + $optionalSecretEnvVars) {
  $enabled = & $Gcloud secrets versions list $name --filter="state=ENABLED" --format="value(name)" 2>$null
  if ($LASTEXITCODE -eq 0 -and $enabled) {
    $pairs += ("{0}={0}:latest" -f $name)
  }
}

# 7) Plain env vars (only those NOT supplied by secrets)
$envPairs = @()   # for --set-env-vars

$projIsSecret = $pairs -match '^PROJECT_ID='
$topicIsSecret = $pairs -match '^PUBSUB_TOPIC_GW_SELF='

if (-not $projIsSecret -and $PROJECT_ID) {
  $envPairs += ("PROJECT_ID={0}" -f $PROJECT_ID)
}
if (-not $topicIsSecret -and $PUBSUB_TOPIC_GW_SELF) {
  $envPairs += ("PUBSUB_TOPIC_GW_SELF={0}" -f $PUBSUB_TOPIC_GW_SELF)
}

# 8) Base deploy args
$deployArgs = @(
  "run","deploy",$SERVICE,
  "--image",$IMAGE,
  "--platform","managed",
  "--region",$REGION,
  "--service-account",$saEmail,
  "--allow-unauthenticated",
  "--port","8080",
  "--cpu","1",
  "--memory","512Mi",
  "--max-instances","3"
)

# Append --set-secrets
if ($pairs.Count -gt 0) {
  $secretsArg = ($pairs -join ",")
  Write-Host "Using --set-secrets: $secretsArg" -ForegroundColor DarkCyan
  $deployArgs += @("--set-secrets", $secretsArg)
} else {
  Write-Warning "No DB_* secrets found; deploying without --set-secrets."
}

# Append --set-env-vars
if ($envPairs.Count -gt 0) {
  $envArg = ($envPairs -join ",")
  Write-Host "Using --set-env-vars: $envArg" -ForegroundColor DarkCyan
  $deployArgs += @("--set-env-vars", $envArg)
} else {
  Write-Warning "No plain env vars to set."
}

# 9) Deploy
Invoke-Cmd $Gcloud $deployArgs

# 10) Output URL + quick hints
$URL = & $Gcloud run services describe $SERVICE --region $REGION --format "value(status.url)"
if ($LASTEXITCODE -ne 0) { throw "Failed to retrieve service URL." }
Write-Host "`n✅ Deployed: $URL" -ForegroundColor Green
Write-Host "`nPoint ble-mqtt-connect to:" -ForegroundColor Yellow
Write-Host "  GWAUTO_ENABLED=true"
Write-Host "  GWAUTO_ENDPOINT=$URL/auto"
Write-Host "  GWAUTO_QUEUE_SIZE=100"
Write-Host "  GWAUTO_TIMEOUT_MS=1500"
Write-Host "  # Authorization is enforced by GWAUTO_AUTH_TOKEN secret (if set)."
