# Configuration for SSH tunnel to remote Docker host
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
    [int]$LocalPort = 23750
)

# Set environment variables for Docker and Testcontainers
$env:SSH_AUTH_SOCK = '\\.\pipe\openssh-ssh-agent'
$env:DOCKER_HOST = "tcp://$($LocalAddress):$($LocalPort)"
$env:TESTCONTAINERS_HOST_OVERRIDE = $RemoteAddress
$env:TESTCONTAINERS_RYUK_DISABLED = 'true'

function Start-DockerTunnel {
    # Check if the configured local port is already being listened on
    $tunnelActive = Get-NetTCPConnection -LocalAddress $LocalAddress -LocalPort $LocalPort -ErrorAction SilentlyContinue

    if ($null -eq $tunnelActive) {
        Write-Host '[Docker] Tunnel is offline. Establishing secure loopback over SSH...' -ForegroundColor Cyan

        Start-Process ssh -ArgumentList '-N', '-L', "$($LocalAddress):$($LocalPort):/var/run/docker.sock", "$RemoteUser@$RemoteAddress" -WindowStyle Hidden

        # Give SSH a moment to negotiate the handshake
        Start-Sleep -Seconds 2

        if (Get-NetTCPConnection -LocalAddress $LocalAddress -LocalPort $LocalPort -ErrorAction SilentlyContinue) {
            Write-Host "[Docker] Tunnel successfully established on port $LocalPort." -ForegroundColor Green
        } else {
            Write-Warning '[Docker] Failed to automatically start the SSH tunnel. Is the remote Docker host online?'
        }
    } else {
        Write-Host "[Docker] Active SSH tunnel detected on port $LocalPort. Ready." -ForegroundColor Green
    }
}

function Stop-DockerTunnel {
    # 1. Locate the local listener on port $LocalPort
    $connection = Get-NetTCPConnection -LocalAddress $LocalAddress -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue

    if ($connection) {
        $pidToKill = $connection.OwningProcess
        # Verify that the process is actually an SSH executable to avoid accidental crossfire
        $process = Get-Process -Id $pidToKill -ErrorAction SilentlyContinue

        if ($process -and $process.Name -eq 'ssh') {
            Stop-Process -Id $pidToKill -Force
            Write-Host "[Docker] Secure SSH tunnel (PID $pidToKill) has been shut down gracefully." -ForegroundColor Yellow
        } else {
            Write-Warning "[Docker] Process ID $pidToKill is using port $LocalPort, but its name is '$($process.Name)'. Skipping kill to protect system safety."
        }
    } else {
        Write-Host "[Docker] No active SSH tunnel detected on port $LocalPort." -ForegroundColor Gray
    }
}

function Get-DockerTunnel {
    # Check current state of the local port
    $connection = Get-NetTCPConnection -LocalAddress $LocalAddress -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue

    if ($connection) {
        $tunnelPid = $connection.OwningProcess
        $process = Get-Process -Id $tunnelPid -ErrorAction SilentlyContinue
        Write-Host '[Docker] Tunnel Status: ACTIVE' -ForegroundColor Green
        Write-Host "  - Endpoint   : $($LocalAddress):$LocalPort"
        Write-Host "  - Process ID : $tunnelPid ($($process.Name).exe)"
    } else {
        Write-Host "[Docker] Tunnel Status: OFFLINE (Port $LocalPort is free)" -ForegroundColor Red
    }
}

Start-DockerTunnel

code .
