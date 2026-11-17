[CmdletBinding(DefaultParameterSetName = 'Start')]
param(
    [Parameter(ParameterSetName = 'Start', Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RemoteUser,

    [Parameter(ParameterSetName = 'Start', Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RemoteAddress,

    [Parameter(ParameterSetName = 'Start')]
    [ValidateNotNullOrEmpty()]
    [ValidatePattern('^/')]
    [string]$RemoteProjectRoot,

    [ValidateNotNullOrEmpty()]
    [string]$LocalAddress = '127.0.0.1',

    [ValidateRange(1, 65535)]
    [int]$DockerPort = 23750,

    [ValidateRange(1, 65535)]
    [int]$SqsPort,

    [ValidateRange(1, 65535)]
    [int]$PostgresPort,

    [Parameter(ParameterSetName = 'Stop', Mandatory = $true)]
    [switch]$Stop,

    [Parameter(ParameterSetName = 'Stop')]
    [switch]$ResetData,

    [Parameter(ParameterSetName = 'Status', Mandatory = $true)]
    [switch]$StatusOnly
)

$dockerSocketPath = '/var/run/docker.sock'
$tunnelStartupTimeoutSeconds = 10
$tunnelPollIntervalMilliseconds = 100
$sshOptions = @(
    '-o', 'BatchMode=yes',
    '-o', 'ExitOnForwardFailure=yes',
    '-o', 'ConnectTimeout=10',
    '-o', 'ServerAliveInterval=30',
    '-o', 'ServerAliveCountMax=3'
)
$startedTunnels = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()

function Assert-DefinitionKeys {
    param([System.Collections.IDictionary]$Value, [string[]]$Allowed, [string[]]$Required, [string]$Context)
    if ($null -eq $Value) {
        throw "Missing object '$Context' in the development definition."
    }
    foreach ($key in $Value.Keys) {
        if ($key -notin $Allowed) {
            throw "Unsupported development definition field '$Context.$key'."
        }
    }
    foreach ($key in $Required) {
        if (-not $Value.Contains($key)) {
            throw "Missing development definition field '$Context.$key'."
        }
    }
}

function Read-DevelopmentDefinition {
    param([Parameter(Mandatory)][string]$Path)
    $json = Get-Content -LiteralPath $Path -Raw -ErrorAction Stop
    if ($json.Contains('$')) {
        throw 'Compose interpolation is unsupported by the remote bootstrap; use literal values in the development definition.'
    }
    $result = ConvertFrom-Json -InputObject $json -AsHashtable -ErrorAction Stop
    Assert-DevelopmentDefinition -Definition $result
    return $result
}

function Assert-DevelopmentDefinition {
    param([System.Collections.IDictionary]$Definition)
    Assert-DefinitionKeys $Definition @('name', 'services', 'volumes') @('name', 'services', 'volumes') 'root'
    Assert-DefinitionKeys $Definition.services @('elasticmq', 'postgres-dev') @('elasticmq', 'postgres-dev') 'services'
    if ($Definition.name -notmatch '^[a-z0-9][a-z0-9_-]*$') {
        throw 'The development project name must be a nonempty Compose project name.'
    }
    foreach ($volume in $Definition.volumes.Values) {
        Assert-DefinitionKeys $volume @('name') @('name') 'volumes'
        if ($volume.name -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]*$') {
            throw 'Named volumes require explicit Docker-compatible names.'
        }
    }
    foreach ($service in $Definition.services.Values) {
        Assert-DefinitionKeys $service @('container_name', 'image', 'restart', 'ports', 'environment', 'volumes', 'healthcheck') `
            @('container_name', 'image', 'restart', 'ports', 'volumes', 'healthcheck') 'services'
        if ($service.container_name -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]*$' -or
            [string]::IsNullOrWhiteSpace($service.image) -or
            $service.restart -notin @('no', 'always', 'unless-stopped', 'on-failure')) {
            throw 'Each development service requires a container name, image, and supported restart policy.'
        }
        if ($service.ports.Count -ne 1) {
            throw 'Each development service must declare exactly one long-form port publication.'
        }
        $port = $service.ports[0]
        Assert-DefinitionKeys $port @('target', 'published', 'host_ip', 'protocol') @('target', 'published', 'host_ip', 'protocol') 'ports'
        if ($port.host_ip -ne '127.0.0.1' -or $port.protocol -ne 'tcp') {
            throw 'Development ports must publish TCP on IPv4 loopback.'
        }
        foreach ($field in @('target', 'published')) {
            $number = 0
            if (-not [int]::TryParse([string]$port[$field], [ref]$number) -or $number -lt 1 -or $number -gt 65535) {
                throw "Invalid development port '$field'."
            }
        }
        if ($service.Contains('environment') -and $service.environment -isnot [System.Collections.IDictionary]) {
            throw 'Development environment values must use mapping syntax.'
        }
        foreach ($value in $service.environment.Values) {
            if ($value -isnot [string]) {
                throw 'Development environment values must be literal strings.'
            }
        }
        foreach ($mount in $service.volumes) {
            Assert-DefinitionKeys $mount @('type', 'source', 'target', 'read_only', 'bind') @('type', 'source', 'target') 'mounts'
            if ($mount.Contains('read_only') -and $mount.read_only -isnot [bool]) {
                throw 'Mount read_only values must be booleans.'
            }
            if ($mount.target -notmatch '^/' -or [string]::IsNullOrWhiteSpace($mount.source)) {
                throw 'Development mounts require a source and an absolute Linux target.'
            }
            switch ($mount.type) {
                'bind' {
                    Assert-DefinitionKeys $mount.bind @('create_host_path') @('create_host_path') 'mounts.bind'
                    if ($mount.source -notmatch '^\./' -or $mount.source -match '(^|/)\.\.(/|$)|\\|,' -or
                        $mount.read_only -ne $true -or $mount.bind.create_host_path -isnot [bool] -or
                        $mount.bind.create_host_path -ne $false) {
                        throw 'Bind mounts must use checkout-relative files, read_only=true, and create_host_path=false.'
                    }
                }
                'volume' {
                    if (-not $Definition.volumes.Contains($mount.source) -or $mount.Contains('bind')) {
                        throw "Unsupported named volume '$($mount.source)'."
                    }
                }
                default { throw "Unsupported development mount type '$($mount.type)'." }
            }
        }
        $health = $service.healthcheck
        Assert-DefinitionKeys $health @('test', 'interval', 'timeout', 'retries', 'start_period') `
            @('test', 'interval', 'timeout', 'retries', 'start_period') 'healthcheck'
        if ($health.test.Count -ne 2 -or $health.test[0] -ne 'CMD-SHELL' -or
            [string]::IsNullOrWhiteSpace($health.test[1]) -or $health.retries -lt 1) {
            throw 'Development health checks require a CMD-SHELL command and positive retry count.'
        }
        foreach ($field in @('interval', 'timeout', 'start_period')) {
            if ($health[$field] -notmatch '^[1-9][0-9]*(ms|s|m|h)$') {
                throw "Unsupported health-check duration '$field'."
            }
        }
    }
}

function Get-ContainerRunArguments {
    param(
        [System.Collections.IDictionary]$Service,
        [System.Collections.IDictionary]$Volumes,
        [System.Collections.IDictionary]$BindSources = @{}
    )
    $arguments = [System.Collections.Generic.List[string]]::new()
    $arguments.AddRange([string[]]@('run', '-d', '--name', $Service.container_name, '--restart', $Service.restart))
    foreach ($port in $Service.ports) {
        $arguments.AddRange([string[]]@('--publish', "$($port.host_ip):$($port.published):$($port.target)/$($port.protocol)"))
    }
    if ($Service.environment) {
        foreach ($entry in $Service.environment.GetEnumerator()) {
            $arguments.AddRange([string[]]@('--env', "$($entry.Key)=$($entry.Value)"))
        }
    }
    foreach ($mount in $Service.volumes) {
        if ($mount.type -eq 'bind') {
            if (-not $BindSources.Contains($mount.source)) {
                throw "Missing Docker-host path for bind source '$($mount.source)'."
            }
            $source = $BindSources[$mount.source]
        } else {
            $source = $Volumes[$mount.source].name
        }
        $specification = "type=$($mount.type),source=$source,target=$($mount.target)"
        if ($mount.read_only) { $specification += ',readonly' }
        $arguments.AddRange([string[]]@('--mount', $specification))
    }
    $health = $Service.healthcheck
    $arguments.AddRange([string[]]@(
        '--health-cmd', $health.test[1], '--health-interval', $health.interval,
        '--health-timeout', $health.timeout, '--health-retries', [string]$health.retries,
        '--health-start-period', $health.start_period, $Service.image
    ))
    return $arguments.ToArray()
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string[]]$ArgumentList,
        [Parameter(Mandatory)][string]$FailureMessage
    )
    try {
        & $FilePath @ArgumentList
    } catch {
        throw [System.InvalidOperationException]::new($FailureMessage, $_.Exception)
    }
    if ($LASTEXITCODE -ne 0) {
        throw "$FailureMessage (exit code $LASTEXITCODE)."
    }
}

function Get-TunnelListener {
    param([int]$Port)
    Get-NetTCPConnection -State Listen -ErrorAction Stop |
        Where-Object { $_.LocalAddress -eq $LocalAddress -and $_.LocalPort -eq $Port }
}

function Stop-TunnelByPort {
    param([int]$Port, [string]$Label)
    $connections = @(Get-TunnelListener -Port $Port)
    if ($connections.Count -gt 0) {
        foreach ($pidToKill in ($connections.OwningProcess | Select-Object -Unique)) {
            $process = Get-Process -Id $pidToKill -ErrorAction SilentlyContinue
            if ($process -and $process.Name -eq 'ssh') {
                Stop-Process -Id $pidToKill -Force -ErrorAction Stop
                Write-Host "[$Label] SSH tunnel (PID $pidToKill) stopped." -ForegroundColor Yellow
            } else {
                Write-Warning "[$Label] Port $Port used by '$($process.Name)' (PID $pidToKill). Skipped kill."
            }
        }
    } else {
        Write-Host "[$Label] No active tunnel detected on port $Port." -ForegroundColor Gray
    }
}

# --- Service & Tunnel Handlers ---

function Start-SshTunnel {
    param([string]$Label, [int]$Port, [string]$Target)
    $forward = "$($LocalAddress):$($Port):$Target"
    $destination = "$RemoteUser@$RemoteAddress"
    $listeners = @(Get-TunnelListener -Port $Port)
    if ($listeners.Count -gt 0) {
        $owners = @($listeners.OwningProcess | Select-Object -Unique)
        if ($owners.Count -eq 1) {
            $owner = Get-CimInstance Win32_Process -Filter "ProcessId=$($owners[0])" -ErrorAction Stop
            $forwardPattern = '(?:^|\s)-L\s+"?' + [regex]::Escape($forward) + '"?(?:\s|$)'
            $destinationPattern = '(?:^|\s)"?' + [regex]::Escape($destination) + '"?(?:\s|$)'
            if ($owner.Name -eq 'ssh.exe' -and
                $owner.CommandLine -match $forwardPattern -and
                $owner.CommandLine -match $destinationPattern) {
                Write-Host "[$Label] Matching SSH tunnel already listening on port $Port." -ForegroundColor Green
                return
            }
        }
        throw "[$Label] Port $Port is occupied by a listener that does not match the requested SSH tunnel."
    }

    Write-Host "[$Label] Establishing SSH tunnel on port $Port..." -ForegroundColor Cyan
    $process = Start-Process ssh -ArgumentList ($sshOptions + @('-N', '-L', $forward, $destination)) `
        -WindowStyle Hidden -PassThru -ErrorAction Stop
    $elapsed = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        while ($elapsed.Elapsed.TotalSeconds -lt $tunnelStartupTimeoutSeconds) {
            if ($process.HasExited) {
                throw "[$Label] SSH exited before tunnel startup (exit code $($process.ExitCode))."
            }
            $listeners = @(Get-TunnelListener -Port $Port)
            if ($listeners.Count -gt 0) {
                if (@($listeners | Where-Object { $_.OwningProcess -ne $process.Id }).Count -gt 0) {
                    throw "[$Label] Another process acquired port $Port during startup."
                }
                $startedTunnels.Add($process)
                Write-Host "[$Label] SSH tunnel listening on port $Port." -ForegroundColor Green
                return
            }
            Start-Sleep -Milliseconds $tunnelPollIntervalMilliseconds
        }
        throw "[$Label] SSH tunnel did not start within $tunnelStartupTimeoutSeconds seconds."
    } catch {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction Stop
        }
        $process.Dispose()
        throw
    }
}

function Assert-ContainerLoopbackBindings {
    param([string]$Name)
    $json = Invoke-NativeCommand -FilePath docker -ArgumentList @(
        'inspect', '--format', '{{json .HostConfig.PortBindings}}', $Name
    ) -FailureMessage "Failed to inspect port bindings for container '$Name'"
    $bindings = ConvertFrom-Json -InputObject $json -AsHashtable -ErrorAction Stop
    if (-not $bindings -or $bindings.Count -eq 0) {
        throw "Container '$Name' has no published development ports. Recreate it with loopback port bindings."
    }
    foreach ($port in $bindings.Keys) {
        foreach ($binding in $bindings[$port]) {
            if ($binding.HostIp -ne '127.0.0.1') {
                throw "Container '$Name' has a port binding outside IPv4 loopback. Recreate it with 127.0.0.1 bindings before startup; preserve any required data."
            }
        }
    }
}

function Ensure-ContainerRunning {
    param([string]$Name, [scriptblock]$Create)
    Write-Host "[Docker] Ensuring '$Name' is running..." -ForegroundColor Cyan
    $filter = 'name=^/' + [regex]::Escape($Name) + '$'
    $running = Invoke-NativeCommand -FilePath docker -ArgumentList @('ps', '-q', '--filter', $filter) `
        -FailureMessage "Failed to query container '$Name'"
    if ($running) {
        Assert-ContainerLoopbackBindings -Name $Name
        return
    }
    $exists = Invoke-NativeCommand -FilePath docker -ArgumentList @('ps', '-aq', '--filter', $filter) `
        -FailureMessage "Failed to query container '$Name'"
    if ($exists) {
        Assert-ContainerLoopbackBindings -Name $Name
        Invoke-NativeCommand -FilePath docker -ArgumentList @('start', $Name) `
            -FailureMessage "Failed to start container '$Name'" | Out-Null
    } else {
        & $Create
        Assert-ContainerLoopbackBindings -Name $Name
    }
}

function Start-DockerServices {
    foreach ($service in $definition.services.Values) {
        Ensure-ContainerRunning -Name $service.container_name -Create {
            $bindSources = @{}
            foreach ($mount in $service.volumes | Where-Object { $_.type -eq 'bind' }) {
                $localFile = Join-Path $PSScriptRoot $mount.source
                if (-not (Test-Path -LiteralPath $localFile -PathType Leaf)) {
                    throw "[Infrastructure] Local config not found at $localFile."
                }
                $remoteFile = "$($RemoteProjectRoot.TrimEnd('/'))/$($mount.source.Substring(2))"
                $remoteDirectory = $remoteFile.Substring(0, $remoteFile.LastIndexOf('/'))
                $quotedDirectory = "'" + $remoteDirectory.Replace("'", "'\''") + "'"
                Invoke-NativeCommand -FilePath ssh -ArgumentList ($sshOptions + @(
                    '-q', "$RemoteUser@$RemoteAddress", "mkdir -p -- $quotedDirectory"
                )) -FailureMessage 'Failed to create the remote configuration directory'
                Invoke-NativeCommand -FilePath scp -ArgumentList ($sshOptions + @(
                    '-q', $localFile, "${RemoteUser}@${RemoteAddress}:${remoteFile}"
                )) -FailureMessage 'Failed to sync the container configuration'
                $bindSources[$mount.source] = $remoteFile
            }
            $arguments = @(Get-ContainerRunArguments $service $definition.volumes $bindSources)
            Invoke-NativeCommand -FilePath docker -ArgumentList $arguments `
                -FailureMessage "Failed to create '$($service.container_name)'" | Out-Null
        }
        Wait-ContainerHealthy -Name $service.container_name
    }
    Write-Host '[Docker] Remote containers healthy.' -ForegroundColor Green
}

function Wait-ContainerHealthy {
    param([string]$Name, [int]$TimeoutSeconds = 90)
    $elapsed = [System.Diagnostics.Stopwatch]::StartNew()
    while ($elapsed.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        $json = Invoke-NativeCommand -FilePath docker -ArgumentList @('inspect', '--format', '{{json .State}}', $Name) `
            -FailureMessage "Failed to inspect readiness for '$Name'"
        $state = ConvertFrom-Json -InputObject $json -ErrorAction Stop
        if (-not $state.Running -or -not $state.Health -or $state.Health.Status -eq 'unhealthy') {
            throw "Container '$Name' is not healthy or lacks its declared health check. Inspect its logs and recreate it from the development definition if needed."
        }
        if ($state.Health.Status -eq 'healthy') { return }
        Start-Sleep -Milliseconds 250
    }
    throw "Container '$Name' did not become healthy within $TimeoutSeconds seconds."
}

function Stop-DockerServices {
    param([switch]$ResetData)
    foreach ($containerName in $definition.services.Values.container_name) {
        $filter = 'name=^/' + [regex]::Escape($containerName) + '$'
        $exists = Invoke-NativeCommand -FilePath docker -ArgumentList @('ps', '-aq', '--filter', $filter) `
            -FailureMessage "Failed to query container '$containerName'"
        if (-not $exists) {
            Write-Host "[Docker] Container '$containerName' is already absent." -ForegroundColor Gray
            continue
        }
        Invoke-NativeCommand -FilePath docker -ArgumentList @('stop', $containerName) `
            -FailureMessage "Failed to stop container '$containerName'" | Out-Null
        Invoke-NativeCommand -FilePath docker -ArgumentList @('rm', $containerName) `
            -FailureMessage "Failed to remove container '$containerName'" | Out-Null
    }
    if ($ResetData) {
        foreach ($volumeName in $definition.volumes.Values.name) {
            $exists = Invoke-NativeCommand -FilePath docker -ArgumentList @(
                'volume', 'ls', '--format', '{{.Name}}', '--filter', "name=^$([regex]::Escape($volumeName))$"
            ) -FailureMessage "Failed to query volume '$volumeName'"
            if ($exists) {
                Invoke-NativeCommand -FilePath docker -ArgumentList @('volume', 'rm', $volumeName) `
                    -FailureMessage "Failed to remove volume '$volumeName'" | Out-Null
            }
        }
    }
}

function Show-TunnelStatus {
    Write-Host "`n--- Tunnel Status ---" -ForegroundColor White
    foreach ($tunnel in $tunnels) {
        $listening = @(Get-TunnelListener -Port $tunnel.Port).Count -gt 0
        $state = if ($listening) { 'LISTENING' } else { 'OFFLINE' }
        $color = if ($listening) { 'Green' } else { 'Red' }
        Write-Host ('{0,-12} (Port {1}): {2}' -f $tunnel.Label, $tunnel.Port, $state) -ForegroundColor $color
    }
    Write-Host "---------------------`n"
}

# --- Execution Router ---

$definition = Read-DevelopmentDefinition -Path (Join-Path $PSScriptRoot 'compose.json')
$elasticMq = $definition.services.elasticmq
$postgres = $definition.services.'postgres-dev'
if (-not $PSBoundParameters.ContainsKey('SqsPort')) { $SqsPort = [int]$elasticMq.ports[0].published }
if (-not $PSBoundParameters.ContainsKey('PostgresPort')) { $PostgresPort = [int]$postgres.ports[0].published }
$tunnels = @(
    @{ Label = 'Docker'; Port = $DockerPort; Target = $dockerSocketPath },
    @{ Label = 'SQS'; Port = $SqsPort; Target = "127.0.0.1:$($elasticMq.ports[0].published)" },
    @{ Label = 'Postgres'; Port = $PostgresPort; Target = "127.0.0.1:$($postgres.ports[0].published)" }
)

if ($StatusOnly) {
    Show-TunnelStatus
    exit 0
}

try {
    $env:DOCKER_HOST = "tcp://$($LocalAddress):$($DockerPort)"
    if ($Stop) {
        Write-Host '[Teardown] Removing development containers...' -ForegroundColor Cyan
        Stop-DockerServices -ResetData:$ResetData
        for ($i = $tunnels.Count - 1; $i -ge 0; $i--) {
            Stop-TunnelByPort -Port $tunnels[$i].Port -Label $tunnels[$i].Label
        }
        exit 0
    }

    if (-not $RemoteProjectRoot) {
        $RemoteProjectRoot = "/home/$RemoteUser/projects/$($definition.name)"
    }
    $env:TESTCONTAINERS_HOST_OVERRIDE = $RemoteAddress

    $dockerTunnel = $tunnels[0]
    Start-SshTunnel @dockerTunnel
    Start-DockerServices
    foreach ($tunnel in $tunnels[1..2]) {
        Start-SshTunnel @tunnel
    }

    Invoke-NativeCommand -FilePath code -ArgumentList @($PSScriptRoot) -FailureMessage 'Failed to launch VS Code'
} catch {
    for ($i = $startedTunnels.Count - 1; $i -ge 0; $i--) {
        if (-not $startedTunnels[$i].HasExited) {
            Stop-Process -Id $startedTunnels[$i].Id -Force -ErrorAction Stop
        }
    }
    Write-Error $_
    exit 1
} finally {
    foreach ($process in $startedTunnels) {
        $process.Dispose()
    }
}
