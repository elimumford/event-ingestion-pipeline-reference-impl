[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RemoteUser,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RemoteAddress,

    [ValidateNotNullOrEmpty()]
    [string]$LocalAddress = '127.0.0.1',

    [ValidateRange(1, 65535)]
    [int]$DockerPort = 23750,

    [ValidateRange(1, 65535)]
    [int]$SqsPort = 9324,

    [ValidateRange(1, 65535)]
    [int]$SqsUiPort = 9325,

    [switch]$Stop,
    [switch]$StatusOnly
)

# Environment variables for Docker CLI and Testcontainers
$env:SSH_AUTH_SOCK = '\\.\pipe\openssh-ssh-agent'
$env:DOCKER_HOST = "tcp://$($LocalAddress):$($DockerPort)"
$env:TESTCONTAINERS_HOST_OVERRIDE = $RemoteAddress
$env:TESTCONTAINERS_RYUK_DISABLED = 'true'

# --- Tunnel Primitives ---

function Test-PortListening {
    param([int]$Port)
    $conn = Get-NetTCPConnection -LocalAddress $LocalAddress -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    return ($null -ne $conn)
}

function Stop-TunnelByPort {
    param([int]$Port, [string]$Label)
    $connection = Get-NetTCPConnection -LocalAddress $LocalAddress -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if ($connection) {
        $pidToKill = $connection.OwningProcess
        $process = Get-Process -Id $pidToKill -ErrorAction SilentlyContinue
        if ($process -and $process.Name -eq 'ssh') {
            Stop-Process -Id $pidToKill -Force
            Write-Host "[$Label] SSH tunnel (PID $pidToKill) stopped." -ForegroundColor Yellow
        } else {
            Write-Warning "[$Label] Port $Port used by '$($process.Name)' (PID $pidToKill). Skipped kill."
        }
    } else {
        Write-Host "[$Label] No active tunnel detected on port $Port." -ForegroundColor Gray
    }
}

# --- Independent Component Handlers ---

function Start-DockerTunnel {
    if (-not (Test-PortListening -Port $DockerPort)) {
        Write-Host '[Docker] Establishing Docker socket tunnel over SSH...' -ForegroundColor Cyan
        Start-Process ssh -ArgumentList '-N', '-L', "$($LocalAddress):$($DockerPort):/var/run/docker.sock", "$RemoteUser@$RemoteAddress" -WindowStyle Hidden
        Start-Sleep -Seconds 2

        if (Test-PortListening -Port $DockerPort) {
            Write-Host "[Docker] Socket tunnel online on port $DockerPort." -ForegroundColor Green
        } else {
            throw "[Docker] Failed to establish Docker tunnel on port $DockerPort."
        }
    } else {
        Write-Host "[Docker] Active socket tunnel detected on port $DockerPort." -ForegroundColor Green
    }
}

function Start-DockerServices {
    Write-Host '[Compose] Starting ElasticMQ on remote daemon via Compose...' -ForegroundColor Cyan
    docker compose up -d sqs
    if ($LASTEXITCODE -ne 0) {
        throw '[Compose] Failed to start services via docker compose.'
    }
    Write-Host '[Compose] ElasticMQ service running.' -ForegroundColor Green
}

function Start-SqsTunnel {
    if (-not (Test-PortListening -Port $SqsPort)) {
        Write-Host '[SQS] Establishing SQS port-forwarding tunnel over SSH...' -ForegroundColor Cyan
        $args = @(
            '-N',
            '-L', "$($LocalAddress):$($SqsPort):127.0.0.1:9324",
            '-L', "$($LocalAddress):$($SqsUiPort):127.0.0.1:9325",
            "$RemoteUser@$RemoteAddress"
        )
        Start-Process ssh -ArgumentList $args -WindowStyle Hidden
        Start-Sleep -Seconds 2

        if (Test-PortListening -Port $SqsPort) {
            Write-Host "[SQS] Tunnels online (API: $SqsPort, UI: $SqsUiPort)." -ForegroundColor Green
        } else {
            throw "[SQS] Failed to establish SQS tunnel on port $SqsPort."
        }
    } else {
        Write-Host "[SQS] Active SQS tunnel detected on port $SqsPort." -ForegroundColor Green
    }
}

function Initialize-SqsQueue {
    param([string]$QueueName = 'inventory-events')
    Write-Host "[SQS] Verifying '$QueueName' queue on localhost:$SqsPort..." -ForegroundColor Cyan

    $maxRetries = 5
    $retry = 0
    $ready = $false

    while (-not $ready -and $retry -lt $maxRetries) {
        try {
            $null = Invoke-RestMethod -Uri "http://${LocalAddress}:${SqsPort}?Action=CreateQueue&QueueName=$QueueName&Version=2012-11-05" -Method Post -ErrorAction Stop
            $ready = $true
            Write-Host "[SQS] Queue ready at http://${LocalAddress}:${SqsPort}/000000000000/$QueueName" -ForegroundColor Green
        } catch {
            $retry++
            Start-Sleep -Milliseconds 500
        }
    }

    if (-not $ready) {
        Write-Warning "[SQS] Timed out waiting for ElasticMQ at http://${LocalAddress}:${SqsPort}."
    }
}

function Show-TunnelStatus {
    $dockerOk = Test-PortListening -Port $DockerPort
    $sqsOk = Test-PortListening -Port $SqsPort

    Write-Host "`n--- Tunnel Status ---" -ForegroundColor White
    Write-Host ('Docker Tunnel (Port {0}): {1}' -f $DockerPort, $(if ($dockerOk) {
                'ACTIVE'
            } else {
                'OFFLINE'
            })) -ForegroundColor $(if ($dockerOk) {
            'Green'
        } else {
            'Red'
        })
    Write-Host ('SQS Tunnel    (Port {0}): {1}' -f $SqsPort, $(if ($sqsOk) {
                'ACTIVE'
            } else {
                'OFFLINE'
            })) -ForegroundColor $(if ($sqsOk) {
            'Green'
        } else {
            'Red'
        })
    Write-Host "---------------------`n"
}

# --- Execution Router ---

if ($StatusOnly) {
    Show-TunnelStatus
    exit 0
}

if ($Stop) {
    Write-Host '[Teardown] Stopping Docker Compose services...' -ForegroundColor Cyan
    docker compose down 2>$null
    Stop-TunnelByPort -Port $SqsPort -Label 'SQS'
    Stop-TunnelByPort -Port $DockerPort -Label 'Docker'
    exit 0
}

# Linear, fail-fast sequence
try {
    Start-DockerTunnel
    Start-DockerServices
    Start-SqsTunnel
    Initialize-SqsQueue -QueueName 'inventory-events'
} catch {
    Write-Error $_
    exit 1
}

code .
