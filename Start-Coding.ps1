[CmdletBinding(DefaultParameterSetName = 'Start')]
param(
    [Parameter(ParameterSetName = 'Start', Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RemoteUser,

    [Parameter(ParameterSetName = 'Start', Mandatory = $true)]
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

    [ValidateRange(1, 65535)]
    [int]$PostgresPort = 5432,

    [Parameter(ParameterSetName = 'Stop', Mandatory = $true)]
    [switch]$Stop,

    [Parameter(ParameterSetName = 'Status', Mandatory = $true)]
    [switch]$StatusOnly
)

# 1. Environment variables for Docker CLI and Testcontainers
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

# --- Service & Tunnel Handlers ---

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
    # 1. ElasticMQ Container
    Write-Host '[Docker] Ensuring ElasticMQ container is running...' -ForegroundColor Cyan
    $sqsRunning = docker ps -q --filter 'name=sqs-emulator'
    if (-not $sqsRunning) {
        $sqsExists = docker ps -aq --filter 'name=sqs-emulator'
        if ($sqsExists) {
            docker start sqs-emulator | Out-Null
        } else {
            docker run -d `
                --name sqs-emulator `
                -p 9324:9324 `
                -p 9325:9325 `
                --restart unless-stopped `
                -e ELASTICMQ_NODE_ADDRESS_PORT=9324 `
                -e ELASTICMQ_NODE_ADDRESS_HOST=localhost `
                softwaremill/elasticmq-native:latest | Out-Null
        }
    }

    # 2. PostgreSQL 17 Container
    Write-Host '[Docker] Ensuring PostgreSQL container is running...' -ForegroundColor Cyan
    $pgRunning = docker ps -q --filter 'name=postgres-dev'
    if (-not $pgRunning) {
        $pgExists = docker ps -aq --filter 'name=postgres-dev'
        if ($pgExists) {
            docker start postgres-dev | Out-Null
        } else {
            docker run -d `
                --name postgres-dev `
                -p 5432:5432 `
                --restart unless-stopped `
                -e POSTGRES_USER=postgres `
                -e POSTGRES_PASSWORD=postgres `
                -e POSTGRES_DB=inventory_db `
                postgres:17-alpine | Out-Null
        }
    }

    if ($LASTEXITCODE -ne 0) {
        throw '[Docker] Failed to start services.'
    }
    Write-Host '[Docker] Remote containers running.' -ForegroundColor Green
}

function Start-SqsTunnel {
    if (-not (Test-PortListening -Port $SqsPort)) {
        Write-Host '[SQS] Establishing SQS port tunnel over SSH...' -ForegroundColor Cyan
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

function Start-PostgresTunnel {
    if (-not (Test-PortListening -Port $PostgresPort)) {
        Write-Host '[Postgres] Establishing PostgreSQL tunnel over SSH...' -ForegroundColor Cyan
        $args = @(
            '-N',
            '-L', "$($LocalAddress):$($PostgresPort):127.0.0.1:5432",
            "$RemoteUser@$RemoteAddress"
        )
        Start-Process ssh -ArgumentList $args -WindowStyle Hidden
        Start-Sleep -Seconds 2

        if (Test-PortListening -Port $PostgresPort) {
            Write-Host "[Postgres] Tunnel online on port $PostgresPort." -ForegroundColor Green
        } else {
            throw "[Postgres] Failed to establish Postgres tunnel on port $PostgresPort."
        }
    } else {
        Write-Host "[Postgres] Active Postgres tunnel detected on port $PostgresPort." -ForegroundColor Green
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
    $postgresOk = Test-PortListening -Port $PostgresPort

    Write-Host "`n--- Tunnel Status ---" -ForegroundColor White
    Write-Host ('Docker Socket (Port {0}): {1}' -f $DockerPort, $(if ($dockerOk) {
                'ACTIVE'
            } else {
                'OFFLINE'
            })) -ForegroundColor $(if ($dockerOk) {
            'Green'
        } else {
            'Red'
        })
    Write-Host ('SQS Emulator  (Port {0}): {1}' -f $SqsPort, $(if ($sqsOk) {
                'ACTIVE'
            } else {
                'OFFLINE'
            })) -ForegroundColor $(if ($sqsOk) {
            'Green'
        } else {
            'Red'
        })
    Write-Host ('PostgreSQL    (Port {0}): {1}' -f $PostgresPort, $(if ($postgresOk) {
                'ACTIVE'
            } else {
                'OFFLINE'
            })) -ForegroundColor $(if ($postgresOk) {
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
    Write-Host '[Teardown] Stopping containers...' -ForegroundColor Cyan
    docker stop sqs-emulator postgres-dev 2>$null
    Stop-TunnelByPort -Port $PostgresPort -Label 'Postgres'
    Stop-TunnelByPort -Port $SqsPort -Label 'SQS'
    Stop-TunnelByPort -Port $DockerPort -Label 'Docker'
    exit 0
}

try {
    Start-DockerTunnel
    Start-DockerServices
    Start-SqsTunnel
    Start-PostgresTunnel
    Initialize-SqsQueue -QueueName 'inventory-events'

    code .
} catch {
    Write-Error $_
    exit 1
}
