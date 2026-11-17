[CmdletBinding()]
param([switch]$Containers)

$ErrorActionPreference = 'Stop'
$scriptPath = Join-Path $PSScriptRoot '..\..\Start-Coding.ps1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $scriptPath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count -gt 0) {
    throw ($parseErrors | Out-String)
}

# Load helpers without executing the bootstrap router or mutating its environment.
foreach ($statement in $ast.EndBlock.Statements) {
    if ($statement -is [System.Management.Automation.Language.FunctionDefinitionAst]) {
        . ([scriptblock]::Create($statement.Extent.Text))
    }
}

function Assert-Condition {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) {
        throw $Message
    }
}

function Assert-Failure {
    param([scriptblock]$Action, [string]$MessagePattern)
    try {
        & $Action
    } catch {
        Assert-Condition ($_.Exception.Message -like $MessagePattern) "Unexpected error: $_"
        return
    }
    throw "Expected failure matching '$MessagePattern'."
}

$definitionPath = Join-Path $PSScriptRoot '..\..\compose.json'
$definition = Read-DevelopmentDefinition -Path $definitionPath
$unsupported = Read-DevelopmentDefinition -Path $definitionPath
$unsupported.services.elasticmq.command = @('unexpected')
Assert-Failure { Assert-DevelopmentDefinition $unsupported } '*Unsupported*command*'
$unsafeDefinition = Read-DevelopmentDefinition -Path $definitionPath
$unsafeDefinition.services.elasticmq.ports[0].host_ip = '0.0.0.0'
Assert-Failure { Assert-DevelopmentDefinition $unsafeDefinition } '*must publish TCP on IPv4 loopback*'
$broker = $definition.services.elasticmq
$bindSource = $broker.volumes[0].source
$arguments = @(Get-ContainerRunArguments $broker $definition.volumes @{
    $bindSource = '/srv/project with spaces/docker/elasticmq/elasticmq.conf'
})
Assert-Condition ($arguments -contains 'type=bind,source=/srv/project with spaces/docker/elasticmq/elasticmq.conf,target=/opt/elasticmq.conf,readonly') `
    'Remote bind path or read-only semantics were lost.'
$changed = Read-DevelopmentDefinition -Path $definitionPath
$changed.services.'postgres-dev'.ports[0].published = '15432'
$changed.services.'postgres-dev'.environment.EXTRA_SETTING = 'value with spaces'
$arguments = @(Get-ContainerRunArguments $changed.services.'postgres-dev' $changed.volumes)
Assert-Condition ($arguments -contains '127.0.0.1:15432:5432/tcp') 'Published port did not come from the definition.'
Assert-Condition ($arguments -contains 'EXTRA_SETTING=value with spaces') 'Environment argument boundaries were lost.'
$temporaryDefinition = Join-Path ([System.IO.Path]::GetTempPath()) "bootstrap-$([guid]::NewGuid().ToString('N')).json"
try {
    $changed.services.elasticmq.image = '${BROKER_IMAGE}'
    [System.IO.File]::WriteAllText($temporaryDefinition, ($changed | ConvertTo-Json -Depth 20))
    Assert-Failure { Read-DevelopmentDefinition -Path $temporaryDefinition } '*interpolation is unsupported*'
} finally {
    Remove-Item -LiteralPath $temporaryDefinition -ErrorAction Stop
}
Write-Host 'PASS: shared definition validation, unsupported-field rejection, and Docker argument translation.'

$shell = (Get-Process -Id $PID).Path
$output = Invoke-NativeCommand -FilePath $shell -ArgumentList @(
    '-NoProfile', '-Command', "Write-Output 'value with spaces'; exit 0"
) -FailureMessage 'Native success case failed'
Assert-Condition ($output -eq 'value with spaces') 'Native stdout or argument boundaries were lost.'
Assert-Failure {
    Invoke-NativeCommand -FilePath $shell -ArgumentList @('-NoProfile', '-Command', 'exit 7') `
        -FailureMessage 'Expected native failure'
} '*Expected native failure*'
Assert-Failure {
    Invoke-NativeCommand -FilePath "missing-$([guid]::NewGuid().ToString('N'))" -ArgumentList @('unused') `
        -FailureMessage 'Expected missing executable'
} '*Expected missing executable*'
Write-Host 'PASS: native stdout, argument preservation, exit failure, and missing executable.'

$environmentNames = @('DOCKER_HOST', 'SSH_AUTH_SOCK', 'SSH_AGENT_PID', 'TESTCONTAINERS_HOST_OVERRIDE', 'TESTCONTAINERS_RYUK_DISABLED')
$originalEnvironment = @{}
try {
    foreach ($name in $environmentNames) {
        $originalEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, "validation-$name", 'Process')
    }
    & $scriptPath -StatusOnly
    Assert-Condition ($LASTEXITCODE -eq 0) 'Status failed.'
    foreach ($name in $environmentNames) {
        Assert-Condition ([Environment]::GetEnvironmentVariable($name, 'Process') -eq "validation-$name") `
            "Status mutated $name."
    }
} finally {
    foreach ($name in $environmentNames) {
        [Environment]::SetEnvironmentVariable($name, $originalEnvironment[$name], 'Process')
    }
}
Write-Host 'PASS: status is environment-neutral.'

$LocalAddress = '127.0.0.1'
$RemoteAddress = '127.0.0.1'
$RemoteUser = "validation-$([guid]::NewGuid().ToString('N'))"
$tunnelStartupTimeoutSeconds = 1
$tunnelPollIntervalMilliseconds = 50
$startedTunnels = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()
$occupied = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$occupied.Start()
try {
    $occupiedPort = $occupied.LocalEndpoint.Port
    Assert-Failure {
        Start-SshTunnel -Label 'Occupied' -Port $occupiedPort -Target '127.0.0.1:1'
    } '*occupied*does not match*'

    try {
        foreach ($name in $environmentNames) {
            $originalEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        }
        $env:SSH_AGENT_PID = 'validation-agent-pid'
        foreach ($agentSocket in @('validation-agent', $null)) {
            [Environment]::SetEnvironmentVariable('SSH_AUTH_SOCK', $agentSocket, 'Process')
            $expectedAgentSocket = [Environment]::GetEnvironmentVariable('SSH_AUTH_SOCK', 'Process')
            foreach ($ryukSetting in @('false', 'true', $null)) {
                [Environment]::SetEnvironmentVariable('TESTCONTAINERS_RYUK_DISABLED', $ryukSetting, 'Process')
                $expectedRyukSetting = [Environment]::GetEnvironmentVariable('TESTCONTAINERS_RYUK_DISABLED', 'Process')
                Assert-Failure {
                    & $scriptPath -RemoteUser $RemoteUser -RemoteAddress $RemoteAddress `
                        -RemoteProjectRoot '/srv/project with spaces' -DockerPort $occupiedPort
                } '*occupied*does not match*'
                Assert-Condition ([Environment]::GetEnvironmentVariable('SSH_AUTH_SOCK', 'Process') -eq $expectedAgentSocket) `
                    'Startup changed the inherited SSH agent socket configuration.'
                Assert-Condition ($env:SSH_AGENT_PID -eq 'validation-agent-pid') 'Startup changed the inherited SSH agent PID.'
                Assert-Condition ([Environment]::GetEnvironmentVariable('TESTCONTAINERS_RYUK_DISABLED', 'Process') -eq $expectedRyukSetting) `
                    'Startup changed Ryuk behavior.'
                Assert-Condition ($env:TESTCONTAINERS_HOST_OVERRIDE -eq $RemoteAddress) 'Startup did not configure Testcontainers.'
            }
        }
    } finally {
        foreach ($name in $environmentNames) {
            [Environment]::SetEnvironmentVariable($name, $originalEnvironment[$name], 'Process')
        }
    }

    $freePort = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $freePort.Start()
    $forwardPort = $freePort.LocalEndpoint.Port
    $freePort.Stop()
    $sshOptions = @('-o', 'BatchMode=yes', '-o', 'ConnectTimeout=30', '-p', "$occupiedPort")
    $elapsed = [System.Diagnostics.Stopwatch]::StartNew()
    Assert-Failure {
        Start-SshTunnel -Label 'Timeout' -Port $forwardPort -Target '127.0.0.1:1'
    } '*did not start within 1 seconds*'
    Assert-Condition ($elapsed.Elapsed.TotalSeconds -lt 6) 'Tunnel startup exceeded its bounded test allowance.'
    $remaining = @(Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" |
        Where-Object { $_.CommandLine -like "*$RemoteUser*" })
    Assert-Condition ($remaining.Count -eq 0) 'Timed-out SSH process was not cleaned up.'
    Assert-Condition (@(Get-TunnelListener -Port $occupiedPort).Count -gt 0) 'An unrelated listener was affected.'

    $sshOptions = @('-o', 'BatchMode=yes', '-o', 'ConnectTimeout=1', '-p', "$forwardPort")
    $tunnelStartupTimeoutSeconds = 5
    Assert-Failure {
        Start-SshTunnel -Label 'EarlyExit' -Port $forwardPort -Target '127.0.0.1:1'
    } '*SSH exited before tunnel startup*'
} finally {
    $occupied.Stop()
}
Write-Host 'PASS: startup preserves supplied or unset SSH agent sockets, the agent PID, and enabled/disabled/unset Ryuk settings.'
Write-Host 'PASS: occupied-port rejection, bounded SSH startup, early exit, and owned-process cleanup.'

if ($Containers) {
    $suffix = [guid]::NewGuid().ToString('N')
    $definition = Read-DevelopmentDefinition -Path $definitionPath
    foreach ($service in $definition.services.Values) {
        $service.container_name = "bootstrap-$suffix-$($service.container_name)"
        $service.ports[0].published = '0'
        $service.restart = 'no'
    }
    $elasticMq = $definition.services.elasticmq
    $postgres = $definition.services.'postgres-dev'
    $dataVolume = "bootstrap-$suffix-postgres-data"
    $definition.volumes.'postgres-data'.name = $dataVolume
    $configVolume = "bootstrap-$suffix-config"
    $decoyName = "$($elasticMq.container_name)-other"
    $unsafeName = "bootstrap-$suffix-unsafe"
    $ownedIds = [System.Collections.Generic.List[string]]::new()
    $ownedVolumes = [System.Collections.Generic.HashSet[string]]::new()
    $null = $ownedVolumes.Add($configVolume)
    $null = $ownedVolumes.Add($dataVolume)
    try {
        $id = Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'create', '--name', "bootstrap-$suffix-config", '--mount',
            "type=volume,source=$configVolume,target=/config", $elasticMq.image
        ) -FailureMessage 'Failed to create configuration carrier'
        $ownedIds.Add($id.Trim())
        $localConfig = Join-Path (Split-Path $scriptPath -Parent) $elasticMq.volumes[0].source
        Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'cp', $localConfig, "$($id.Trim()):/config/elasticmq.conf"
        ) -FailureMessage 'Failed to copy the checked-in broker configuration'
        $configDirectory = Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'volume', 'inspect', '--format', '{{.Mountpoint}}', $configVolume
        ) -FailureMessage 'Failed to locate the disposable Docker-host configuration'
        $bindSources = @{ $elasticMq.volumes[0].source = "$configDirectory/elasticmq.conf" }

        foreach ($service in $definition.services.Values) {
            Ensure-ContainerRunning -Name $service.container_name -Create {
                $arguments = @(Get-ContainerRunArguments $service $definition.volumes $bindSources)
                $id = Invoke-NativeCommand -FilePath docker -ArgumentList $arguments `
                    -FailureMessage 'Failed to create service from the shared definition'
                $ownedIds.Add($id.Trim())
            }
            Wait-ContainerHealthy -Name $service.container_name
            $mountsJson = Invoke-NativeCommand -FilePath docker -ArgumentList @(
                'inspect', '--format', '{{json .Mounts}}', $service.container_name
            ) -FailureMessage 'Failed to inspect disposable volume ownership'
            foreach ($mount in (ConvertFrom-Json -InputObject $mountsJson -ErrorAction Stop)) {
                if ($mount.Type -eq 'volume') { $null = $ownedVolumes.Add($mount.Name) }
            }
        }
        $id = Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'run', '-d', '--name', $decoyName, $elasticMq.image
        ) -FailureMessage 'Failed to create disposable decoy'
        $ownedIds.Add($id.Trim())

        # The wildcard publication targets an unused container port, not a broker listener.
        $id = Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'create', '--name', $unsafeName, '-p', "127.0.0.1::$($elasticMq.ports[0].target)", '-p', '9',
            $elasticMq.image
        ) -FailureMessage 'Failed to create disposable unsafe-binding case'
        $ownedIds.Add($id.Trim())
        Assert-Failure {
            Ensure-ContainerRunning -Name $unsafeName -Create { throw 'Existing container should have been inspected.' }
        } '*outside IPv4 loopback*Recreate*'
        $unsafeRunning = Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'inspect', '--format', '{{.State.Running}}', $unsafeName
        ) -FailureMessage 'Failed to inspect rejected container'
        Assert-Condition ($unsafeRunning -eq 'false') 'Container with unsafe bindings was started.'

        Invoke-NativeCommand -FilePath docker -ArgumentList @('stop', $postgres.container_name) `
            -FailureMessage 'Failed to stop disposable PostgreSQL' | Out-Null
        Ensure-ContainerRunning -Name $postgres.container_name -Create { throw 'Stopped container should have been reused.' }
        Wait-ContainerHealthy -Name $postgres.container_name
        foreach ($name in @($elasticMq.container_name, $postgres.container_name)) {
            Ensure-ContainerRunning -Name $name -Create { throw 'Running container should have been reused.' }
            $running = Invoke-NativeCommand -FilePath docker -ArgumentList @(
                'inspect', '--format', '{{.State.Running}}', $name
            ) -FailureMessage 'Failed to inspect disposable container'
            Assert-Condition ($running -eq 'true') "Container '$name' was not started."
            $portsJson = Invoke-NativeCommand -FilePath docker -ArgumentList @(
                'inspect', '--format', '{{json .NetworkSettings.Ports}}', $name
            ) -FailureMessage 'Failed to inspect live disposable port bindings'
            $ports = ConvertFrom-Json -InputObject $portsJson -AsHashtable -ErrorAction Stop
            $publishedBindings = @($ports.Values | ForEach-Object { $_ } | Where-Object { $null -ne $_ })
            Assert-Condition ($publishedBindings.Count -gt 0) "Container '$name' has no live published port."
            foreach ($binding in $publishedBindings) {
                Assert-Condition ($binding.HostIp -eq '127.0.0.1') "Container '$name' has a non-loopback publication."
            }
        }
        Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'exec', $postgres.container_name, 'psql', '-U', $postgres.environment.POSTGRES_USER,
            '-d', $postgres.environment.POSTGRES_DB, '-v', 'ON_ERROR_STOP=1', '-c',
            'CREATE TABLE bootstrap_probe (id integer); INSERT INTO bootstrap_probe VALUES (1);'
        ) -FailureMessage 'Failed to seed disposable persistence probe' | Out-Null
        Stop-DockerServices
        Stop-DockerServices
        foreach ($name in @($elasticMq.container_name, $postgres.container_name)) {
            $remaining = Invoke-NativeCommand -FilePath docker -ArgumentList @(
                'ps', '-aq', '--filter', "name=^/$name`$"
            ) -FailureMessage 'Failed to verify teardown'
            Assert-Condition (-not $remaining) "Container '$name' survived teardown."
        }
        $decoyRunning = Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'inspect', '--format', '{{.State.Running}}', $decoyName
        ) -FailureMessage 'Failed to inspect decoy'
        Assert-Condition ($decoyRunning -eq 'true') 'Similarly named container was affected.'

        $arguments = @(Get-ContainerRunArguments $postgres $definition.volumes)
        $id = Invoke-NativeCommand -FilePath docker -ArgumentList $arguments -FailureMessage 'Failed to recreate PostgreSQL'
        $ownedIds.Add($id.Trim())
        Wait-ContainerHealthy -Name $postgres.container_name
        $count = Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'exec', $postgres.container_name, 'psql', '-U', $postgres.environment.POSTGRES_USER,
            '-d', $postgres.environment.POSTGRES_DB, '-Atc', 'SELECT count(*) FROM bootstrap_probe;'
        ) -FailureMessage 'Failed to verify retained development data'
        Assert-Condition ($count -eq '1') 'Ordinary teardown did not preserve the named data volume.'
        Stop-DockerServices -ResetData
        Stop-DockerServices -ResetData
        $remainingVolume = Invoke-NativeCommand -FilePath docker -ArgumentList @(
            'volume', 'ls', '--format', '{{.Name}}', '--filter', "name=^$dataVolume`$"
        ) -FailureMessage 'Failed to verify explicit data reset'
        Assert-Condition (-not $remainingVolume) 'Explicit reset left the declared data volume behind.'
    } finally {
        foreach ($id in $ownedIds) {
            $remaining = Invoke-NativeCommand -FilePath docker -ArgumentList @('ps', '-aq', '--filter', "id=$id") `
                -FailureMessage 'Failed to query disposable container during cleanup'
            if ($remaining) {
                Invoke-NativeCommand -FilePath docker -ArgumentList @('rm', '-f', '-v', $id) `
                    -FailureMessage 'Failed to clean up disposable container' | Out-Null
            }
        }
        $existingVolumes = @(Invoke-NativeCommand -FilePath docker -ArgumentList @('volume', 'ls', '--format', '{{.Name}}') `
            -FailureMessage 'Failed to inspect disposable volumes during cleanup')
        foreach ($volume in $ownedVolumes) {
            if ($existingVolumes -contains $volume) {
                Invoke-NativeCommand -FilePath docker -ArgumentList @('volume', 'rm', $volume) `
                    -FailureMessage 'Failed to remove disposable volume' | Out-Null
            }
        }
    }
    Write-Host 'PASS: definition-driven services and health checks, private bindings, safe reuse, data retention/reset, exact-name isolation, and cleanup.'
}
