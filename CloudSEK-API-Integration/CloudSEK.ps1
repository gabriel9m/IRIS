[CmdletBinding()]
param(
    [string]$ConfigPath = 'C:\CloudSEK\state\cloudsek-config.json',
    [ValidateSet('LivePoll', 'PendingOnly', 'RetryFailed')]
    [string]$Mode = 'LivePoll',
    [switch]$RunOnce
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-UtcNowString {
    return [DateTime]::UtcNow.ToString('o')
}

function Get-NowFileStamp {
    return [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
}

function ConvertTo-JsonSafe {
    param(
        [Parameter(Mandatory)]
        $InputObject,
        [switch]$Compress
    )

    if ($Compress) {
        return ($InputObject | ConvertTo-Json -Depth 100 -Compress)
    }

    return ($InputObject | ConvertTo-Json -Depth 100)
}

function Save-JsonFile {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [Parameter(Mandatory)]
        $Object
    )

    $directory = Split-Path -Path $Path -Parent
    if (-not [string]::IsNullOrWhiteSpace($directory) -and -not (Test-Path -LiteralPath $directory)) {
        $null = New-Item -Path $directory -ItemType Directory -Force
    }

    $json = ConvertTo-JsonSafe -InputObject $Object
    [System.IO.File]::WriteAllText($Path, $json, [System.Text.UTF8Encoding]::new($false))
}

function Read-JsonFileOrDefault {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [AllowNull()]
        $DefaultObject = $null
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $DefaultObject
    }

    $raw = [System.IO.File]::ReadAllText($Path, [System.Text.UTF8Encoding]::new($false))
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return $DefaultObject
    }

    try {
        return ($raw | ConvertFrom-Json)
    }
    catch {
        return $DefaultObject
    }
}

function Get-FirstNonEmpty {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        $Values
    )

    foreach ($value in $Values) {
        if ($null -eq $value) { continue }
        $text = [string]$value
        if (-not [string]::IsNullOrWhiteSpace($text)) {
            return $text.Trim()
        }
    }

    return $null
}

function Get-SafeValue {
    param(
        [Parameter(Mandatory)]
        [scriptblock]$ScriptBlock
    )

    try {
        return (& $ScriptBlock)
    }
    catch {
        return $null
    }
}

function Get-SafeString {
    param(
        $Value
    )

    if ($null -eq $Value) {
        return $null
    }

    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    return $text.Trim()
}

function Get-RecordSourceRef {
    param(
        $Object
    )

    if ($null -eq $Object) {
        return $null
    }

    if ($Object -is [System.Collections.IDictionary]) {
        if ($Object.Contains('source_ref')) {
            return (Get-SafeString $Object['source_ref'])
        }
        return $null
    }

    return (Get-SafeString (Get-SafeValue { $Object.source_ref }))
}

function Get-RecordRawFile {
    param(
        $Object
    )

    if ($null -eq $Object) {
        return $null
    }

    if ($Object -is [System.Collections.IDictionary]) {
        if ($Object.Contains('raw_file')) {
            return (Get-SafeString $Object['raw_file'])
        }
        return $null
    }

    return (Get-SafeString (Get-SafeValue { $Object.raw_file }))
}

function Copy-ObjectToOrderedRecord {
    param(
        $Object
    )

    $record = [ordered]@{}
    if ($null -eq $Object) {
        return $record
    }

    if ($Object -is [System.Collections.IDictionary]) {
        foreach ($key in $Object.Keys) {
            $record[[string]$key] = $Object[$key]
        }
        return $record
    }

    foreach ($property in $Object.PSObject.Properties) {
        $record[$property.Name] = $property.Value
    }

    return $record
}

function Normalize-QueueStateObject {
    param(
        $QueueState
    )

    $normalized = [ordered]@{
        version = 1
        items   = @()
    }

    if ($null -eq $QueueState) {
        return $normalized
    }

    $version = Get-SafeValue { $QueueState.version }
    if ($null -ne $version) {
        $normalized.version = $version
    }

    $items = @()
    $rawItems = Get-SafeValue { $QueueState.items }
    foreach ($item in @($rawItems)) {
        if ($null -eq $item) { continue }

        $record = Copy-ObjectToOrderedRecord -Object $item
        $sourceRef = Get-RecordSourceRef -Object $item
        $rawFile = Get-RecordRawFile -Object $item

        if (-not $record.Contains('source_ref') -and -not [string]::IsNullOrWhiteSpace($sourceRef)) {
            $record['source_ref'] = $sourceRef
        }

        if (-not $record.Contains('raw_file') -and -not [string]::IsNullOrWhiteSpace($rawFile)) {
            $record['raw_file'] = $rawFile
        }

        if ([string]::IsNullOrWhiteSpace((Get-RecordSourceRef -Object $record)) -and [string]::IsNullOrWhiteSpace((Get-RecordRawFile -Object $record))) {
            continue
        }

        $items += ,$record
    }

    $normalized.items = @($items)
    return $normalized
}

function Normalize-SentStateObject {
    param(
        $SentState
    )

    $normalized = [ordered]@{
        version = 1
        items   = @()
    }

    if ($null -eq $SentState) {
        return $normalized
    }

    $version = Get-SafeValue { $SentState.version }
    if ($null -ne $version) {
        $normalized.version = $version
    }

    $items = @()
    $rawItems = Get-SafeValue { $SentState.items }
    foreach ($item in @($rawItems)) {
        if ($null -eq $item) { continue }

        $record = Copy-ObjectToOrderedRecord -Object $item
        $sourceRef = Get-RecordSourceRef -Object $item

        if (-not $record.Contains('source_ref') -and -not [string]::IsNullOrWhiteSpace($sourceRef)) {
            $record['source_ref'] = $sourceRef
        }

        if ([string]::IsNullOrWhiteSpace((Get-RecordSourceRef -Object $record))) {
            continue
        }

        $items += ,$record
    }

    $normalized.items = @($items)
    return $normalized
}

function Limit-Text {
    param(
        [string]$Text,
        [int]$MaxLength = 400
    )

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }

    if ($Text.Length -le $MaxLength) {
        return $Text
    }

    return ($Text.Substring(0, $MaxLength - 3) + '...')
}

function Get-SafeFileToken {
    param(
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return 'no-ref'
    }

    $token = $Value -replace '[^A-Za-z0-9._-]', '_'
    $token = $token.Trim('_')
    if ([string]::IsNullOrWhiteSpace($token)) {
        return 'no-ref'
    }

    return (Limit-Text -Text $token -MaxLength 120)
}

function Get-StringSha256 {
    param(
        [Parameter(Mandatory)]
        [string]$Text
    )

    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
        $hash = $sha.ComputeHash($bytes)
        return ([BitConverter]::ToString($hash) -replace '-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

function Get-RootPaths {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath
    )

    $workRoot = Join-Path -Path $RootPath -ChildPath 'work'

    return [ordered]@{
        Root            = $RootPath
        Incoming        = Join-Path -Path $RootPath -ChildPath 'incoming'
        Failed          = Join-Path -Path $RootPath -ChildPath 'failed'
        Logs            = Join-Path -Path $RootPath -ChildPath 'logs'
        State           = Join-Path -Path $RootPath -ChildPath 'state'
        Work            = $workRoot
        WorkNormalized  = Join-Path -Path $workRoot -ChildPath 'normalized'
        WorkRequests    = Join-Path -Path $workRoot -ChildPath 'requests'
        WorkResponses   = Join-Path -Path $workRoot -ChildPath 'responses'
        QueueFile       = Join-Path -Path (Join-Path -Path $RootPath -ChildPath 'state') -ChildPath 'queue.json'
        SentFile        = Join-Path -Path (Join-Path -Path $RootPath -ChildPath 'state') -ChildPath 'sent-alerts.json'
        FailureLogFile  = Join-Path -Path (Join-Path -Path $RootPath -ChildPath 'failed') -ChildPath 'failure-log.jsonl'
    }
}

function Ensure-DirectoryStructure {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath
    )

    $paths = Get-RootPaths -RootPath $RootPath
    foreach ($path in @(
        $paths.Root,
        $paths.Incoming,
        $paths.Failed,
        $paths.Logs,
        $paths.State,
        $paths.Work,
        $paths.WorkNormalized,
        $paths.WorkRequests,
        $paths.WorkResponses
    )) {
        if (-not (Test-Path -LiteralPath $path)) {
            $null = New-Item -Path $path -ItemType Directory -Force
        }
    }

    if (-not (Test-Path -LiteralPath $paths.QueueFile)) {
        Save-JsonFile -Path $paths.QueueFile -Object ([ordered]@{ version = 1; items = @() })
    }

    if (-not (Test-Path -LiteralPath $paths.SentFile)) {
        Save-JsonFile -Path $paths.SentFile -Object ([ordered]@{ version = 1; items = @() })
    }

    if (-not (Test-Path -LiteralPath $paths.FailureLogFile)) {
        [System.IO.File]::WriteAllText($paths.FailureLogFile, '', [System.Text.UTF8Encoding]::new($false))
    }

    return $paths
}

function Write-OperationalLog {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath,
        [Parameter(Mandatory)]
        [string]$Level,
        [Parameter(Mandatory)]
        [string]$Message,
        $Context
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $logFile = Join-Path -Path $paths.Logs -ChildPath ([DateTime]::UtcNow.ToString('yyyy-MM-dd') + '.log')
    $entry = [ordered]@{
        timestamp = Get-UtcNowString
        level     = $Level
        message   = $Message
        context   = $Context
    }
    $line = ConvertTo-JsonSafe -InputObject $entry -Compress
    Add-Content -Path $logFile -Value $line -Encoding UTF8

    $consoleColor = switch ($Level.ToUpperInvariant()) {
        'ERROR' { 'Red' }
        'WARN'  { 'Yellow' }
        'INFO'  { 'Cyan' }
        default { 'Gray' }
    }

    Write-Host ('[{0}] {1}' -f $Level.ToUpperInvariant(), $Message) -ForegroundColor $consoleColor
}

function New-SampleConfigObject {
    param(
        [string]$RootPath = 'C:\CloudSEK'
    )

    return [ordered]@{
        RootPath = $RootPath
        CloudSEK = [ordered]@{
            BaseUrl         = 'https://api.cloudsek.com'
            ApiKey          = '<REQUIRED>'
            Limit           = 100
            Page            = 1
            IntervalSeconds = 300
            ModuleName      = @()
        }
        IRIS = [ordered]@{
            BaseUrl               = 'https://10.96.123.80'
            CreateUri             = 'https://10.96.123.80/alerts/add?cid=1'
            ApiKey                = '<REQUIRED>'
            TimeoutSec            = 60
            SkipTlsValidation     = $true
            AlertCustomerId       = 1
            AlertClassificationId = 1
            SeverityMap           = [ordered]@{
                P0      = 1
                P1      = 1
                P2      = 4
                Default = 2
            }
            StatusMap             = [ordered]@{
                open         = 1
                acknowledged = 1
                in_progress  = 1
                reopened     = 1
                closed       = 1
                default      = 1
            }
        }
    }
}

function Ensure-ConfigFile {
    param(
        [Parameter(Mandatory)]
        [string]$ConfigPath
    )

    if (Test-Path -LiteralPath $ConfigPath) {
        return $false
    }

    $directory = Split-Path -Path $ConfigPath -Parent
    if (-not [string]::IsNullOrWhiteSpace($directory) -and -not (Test-Path -LiteralPath $directory)) {
        $null = New-Item -Path $directory -ItemType Directory -Force
    }

    $sample = New-SampleConfigObject
    Save-JsonFile -Path $ConfigPath -Object $sample
    Write-Host ("Config file created at '{0}'. Fill CloudSEK.ApiKey and IRIS.ApiKey, run the separate discovery script CloudSEK-Discover-IrisMappings.ps1 to discover the real IRIS severity/status IDs, then run again." -f $ConfigPath) -ForegroundColor Yellow
    return $true
}

function Test-IsPlaceholderValue {
    param(
        $Value
    )

    if ($null -eq $Value) {
        return $true
    }

    $text = [string]$Value
    return ($text -match '^<REQUIRED')
}

function Convert-ToRequiredInt {
    param(
        $Value,
        [Parameter(Mandatory)]
        [string]$FieldName
    )

    if (Test-IsPlaceholderValue -Value $Value) {
        throw "Field '$FieldName' is still using a placeholder and needs a validated environment ID."
    }

    try {
        return [int]$Value
    }
    catch {
        throw "Field '$FieldName' must be an integer. Current value: '$Value'."
    }
}

function Load-ValidatedConfig {
    param(
        [Parameter(Mandatory)]
        [string]$ConfigPath
    )

    $configCreated = Ensure-ConfigFile -ConfigPath $ConfigPath
    if ($configCreated) {
        return $null
    }

    $config = Read-JsonFileOrDefault -Path $ConfigPath -DefaultObject $null
    if ($null -eq $config) {
        throw "Could not read config file '$ConfigPath'."
    }

    if (Test-IsPlaceholderValue -Value $config.RootPath) {
        throw 'RootPath cannot stay as placeholder.'
    }

    if (Test-IsPlaceholderValue -Value $config.CloudSEK.ApiKey) {
        throw 'CloudSEK.ApiKey cannot stay as placeholder.'
    }

    if (Test-IsPlaceholderValue -Value $config.IRIS.ApiKey) {
        throw 'IRIS.ApiKey cannot stay as placeholder.'
    }

    if (Test-IsPlaceholderValue -Value $config.IRIS.CreateUri) {
        throw 'IRIS.CreateUri cannot stay as placeholder.'
    }

    $config.CloudSEK.Limit = Convert-ToRequiredInt -Value $config.CloudSEK.Limit -FieldName 'CloudSEK.Limit'
    $config.CloudSEK.Page = Convert-ToRequiredInt -Value $config.CloudSEK.Page -FieldName 'CloudSEK.Page'
    $config.CloudSEK.IntervalSeconds = Convert-ToRequiredInt -Value $config.CloudSEK.IntervalSeconds -FieldName 'CloudSEK.IntervalSeconds'
    $config.IRIS.TimeoutSec = Convert-ToRequiredInt -Value $config.IRIS.TimeoutSec -FieldName 'IRIS.TimeoutSec'
    $config.IRIS.AlertCustomerId = Convert-ToRequiredInt -Value $config.IRIS.AlertCustomerId -FieldName 'IRIS.AlertCustomerId'
    $config.IRIS.AlertClassificationId = Convert-ToRequiredInt -Value $config.IRIS.AlertClassificationId -FieldName 'IRIS.AlertClassificationId'

    $config.IRIS.SeverityMap.P0 = Convert-ToRequiredInt -Value $config.IRIS.SeverityMap.P0 -FieldName 'IRIS.SeverityMap.P0'
    $config.IRIS.SeverityMap.P1 = Convert-ToRequiredInt -Value $config.IRIS.SeverityMap.P1 -FieldName 'IRIS.SeverityMap.P1'
    $config.IRIS.SeverityMap.P2 = Convert-ToRequiredInt -Value $config.IRIS.SeverityMap.P2 -FieldName 'IRIS.SeverityMap.P2'
    $config.IRIS.SeverityMap.Default = Convert-ToRequiredInt -Value $config.IRIS.SeverityMap.Default -FieldName 'IRIS.SeverityMap.Default'

    $config.IRIS.StatusMap.open = Convert-ToRequiredInt -Value $config.IRIS.StatusMap.open -FieldName 'IRIS.StatusMap.open'
    $config.IRIS.StatusMap.acknowledged = Convert-ToRequiredInt -Value $config.IRIS.StatusMap.acknowledged -FieldName 'IRIS.StatusMap.acknowledged'
    $config.IRIS.StatusMap.in_progress = Convert-ToRequiredInt -Value $config.IRIS.StatusMap.in_progress -FieldName 'IRIS.StatusMap.in_progress'
    $config.IRIS.StatusMap.reopened = Convert-ToRequiredInt -Value $config.IRIS.StatusMap.reopened -FieldName 'IRIS.StatusMap.reopened'
    $config.IRIS.StatusMap.closed = Convert-ToRequiredInt -Value $config.IRIS.StatusMap.closed -FieldName 'IRIS.StatusMap.closed'
    $config.IRIS.StatusMap.default = Convert-ToRequiredInt -Value $config.IRIS.StatusMap.default -FieldName 'IRIS.StatusMap.default'

    return $config
}

function Read-QueueState {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $state = Read-JsonFileOrDefault -Path $paths.QueueFile -DefaultObject ([ordered]@{ version = 1; items = @() })
    return (Normalize-QueueStateObject -QueueState $state)
}

function Save-QueueState {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath,
        [Parameter(Mandatory)]
        $QueueState
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $normalized = Normalize-QueueStateObject -QueueState $QueueState
    Save-JsonFile -Path $paths.QueueFile -Object $normalized
}

function Read-SentState {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $state = Read-JsonFileOrDefault -Path $paths.SentFile -DefaultObject ([ordered]@{ version = 1; items = @() })
    return (Normalize-SentStateObject -SentState $state)
}

function Save-SentState {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath,
        [Parameter(Mandatory)]
        $SentState
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $normalized = Normalize-SentStateObject -SentState $SentState
    Save-JsonFile -Path $paths.SentFile -Object $normalized
}

function Get-SentRecord {
    param(
        [Parameter(Mandatory)]
        $SentState,
        [Parameter(Mandatory)]
        [string]$SourceRef
    )

    foreach ($item in @($SentState.items)) {
        if ((Get-RecordSourceRef -Object $item) -eq $SourceRef) {
            return $item
        }
    }

    return $null
}

function Upsert-SentRecord {
    param(
        [Parameter(Mandatory)]
        $SentState,
        [Parameter(Mandatory)]
        $Record
    )

    $items = [System.Collections.ArrayList]::new()
    $updated = $false

    foreach ($item in @($SentState.items)) {
        if ((Get-RecordSourceRef -Object $item) -eq (Get-RecordSourceRef -Object $Record)) {
            $null = $items.Add($Record)
            $updated = $true
        }
        else {
            $null = $items.Add($item)
        }
    }

    if (-not $updated) {
        $null = $items.Add($Record)
    }

    $SentState.items = @($items)
}

function Upsert-QueueRecord {
    param(
        [Parameter(Mandatory)]
        $QueueState,
        [Parameter(Mandatory)]
        $Record
    )

    $items = [System.Collections.ArrayList]::new()
    $updated = $false

    foreach ($item in @($QueueState.items)) {
        if (((Get-RecordSourceRef -Object $item) -eq (Get-RecordSourceRef -Object $Record)) -or ((Get-RecordRawFile -Object $item) -eq (Get-RecordRawFile -Object $Record))) {
            $null = $items.Add($Record)
            $updated = $true
        }
        else {
            $null = $items.Add($item)
        }
    }

    if (-not $updated) {
        $null = $items.Add($Record)
    }

    $QueueState.items = @($items)
}

function Update-QueueRecordStatus {
    param(
        [Parameter(Mandatory)]
        $QueueState,
        [Parameter(Mandatory)]
        [string]$SourceRef,
        [Parameter(Mandatory)]
        [string]$Status,
        [string]$RawFile,
        [string]$Location,
        [string]$NormalizedFile,
        [string]$RequestFile,
        [string]$ResponseFile,
        [Nullable[int]]$AttemptCount,
        [Nullable[int]]$LastHttpStatus,
        [string]$LastErrorReason,
        [string]$LastErrorDetail,
        [Nullable[int]]$IrisAlertId
    )

    $record = $null
    foreach ($item in @($QueueState.items)) {
        if ((Get-RecordSourceRef -Object $item) -eq $SourceRef) {
            $record = Copy-ObjectToOrderedRecord -Object $item
            break
        }
    }

    if ($null -eq $record) {
        $record = [ordered]@{
            source_ref        = $SourceRef
            raw_file          = $RawFile
            location          = $Location
            status            = $Status
            first_seen_at     = Get-UtcNowString
            last_updated_at   = Get-UtcNowString
            attempts          = $(if ($null -ne $AttemptCount) { $AttemptCount } else { 0 })
            last_http_status  = $LastHttpStatus
            last_error_reason = $LastErrorReason
            last_error_detail = $LastErrorDetail
            normalized_file   = $NormalizedFile
            request_file      = $RequestFile
            response_file     = $ResponseFile
            iris_alert_id     = $IrisAlertId
        }
    }
    else {
        if ($PSBoundParameters.ContainsKey('RawFile') -and -not [string]::IsNullOrWhiteSpace($RawFile)) { $record.raw_file = $RawFile }
        if ($PSBoundParameters.ContainsKey('Location') -and -not [string]::IsNullOrWhiteSpace($Location)) { $record.location = $Location }
        if ($PSBoundParameters.ContainsKey('NormalizedFile') -and -not [string]::IsNullOrWhiteSpace($NormalizedFile)) { $record.normalized_file = $NormalizedFile }
        if ($PSBoundParameters.ContainsKey('RequestFile') -and -not [string]::IsNullOrWhiteSpace($RequestFile)) { $record.request_file = $RequestFile }
        if ($PSBoundParameters.ContainsKey('ResponseFile') -and -not [string]::IsNullOrWhiteSpace($ResponseFile)) { $record.response_file = $ResponseFile }
        if ($PSBoundParameters.ContainsKey('AttemptCount') -and $null -ne $AttemptCount) { $record.attempts = $AttemptCount }
        if ($PSBoundParameters.ContainsKey('LastHttpStatus')) { $record.last_http_status = $LastHttpStatus }
        if ($PSBoundParameters.ContainsKey('LastErrorReason')) { $record.last_error_reason = $LastErrorReason }
        if ($PSBoundParameters.ContainsKey('LastErrorDetail')) { $record.last_error_detail = $LastErrorDetail }
        if ($PSBoundParameters.ContainsKey('IrisAlertId')) { $record.iris_alert_id = $IrisAlertId }
        $record.status = $Status
        $record.last_updated_at = Get-UtcNowString
    }

    Upsert-QueueRecord -QueueState $QueueState -Record $record
}

function New-CloudSEKHeaders {
    param(
        [Parameter(Mandatory)]
        [string]$ApiKey
    )

    return @{
        'Authorization' = "Bearer $ApiKey"
        'Accept'        = 'application/json'
    }
}

function New-CloudSEKAlertsUri {
    param(
        [Parameter(Mandatory)]
        [string]$BaseUrl,
        [Parameter(Mandatory)]
        [int]$Limit,
        [Parameter(Mandatory)]
        [int]$Page,
        [string[]]$ModuleName
    )

    $builder = [System.UriBuilder]::new($BaseUrl)
    $basePath = $builder.Path.TrimEnd('/')
    if ([string]::IsNullOrWhiteSpace($basePath) -or $basePath -eq '/') {
        $builder.Path = '/incidents/alerts'
    }
    else {
        $builder.Path = "$basePath/incidents/alerts"
    }

    $query = [System.Collections.Generic.List[string]]::new()
    $query.Add('limit=' + [uri]::EscapeDataString([string]$Limit))
    $query.Add('page=' + [uri]::EscapeDataString([string]$Page))

    foreach ($module in @($ModuleName)) {
        $moduleText = Get-SafeString -Value $module
        if (-not [string]::IsNullOrWhiteSpace($moduleText)) {
            $query.Add('module_name=' + [uri]::EscapeDataString($moduleText))
        }
    }

    $builder.Query = ($query -join '&')
    return $builder.Uri.AbsoluteUri
}


function Get-CompatibleSecurityProtocol {
    $protocol = [System.Net.SecurityProtocolType]0
    foreach ($name in @('Tls', 'Tls11', 'Tls12')) {
        try {
            $protocol = $protocol -bor ([System.Net.SecurityProtocolType]::$name)
        }
        catch {
        }
    }

    if ([int]$protocol -eq 0) {
        try {
            $protocol = [System.Net.ServicePointManager]::SecurityProtocol
        }
        catch {
        }
    }

    return $protocol
}

function Invoke-HttpWebRequestCompat {
    param(
        [Parameter(Mandatory)]
        [ValidateSet('GET', 'POST')]
        [string]$Method,
        [Parameter(Mandatory)]
        [string]$Uri,
        [hashtable]$Headers,
        [string]$Body,
        [Nullable[int]]$TimeoutSec,
        [switch]$SkipTlsValidation
    )

    $request = [System.Net.HttpWebRequest]::Create($Uri)
    $request.Method = $Method
    $request.Accept = 'application/json'
    $request.KeepAlive = $false
    $request.ProtocolVersion = [System.Version]::new(1,1)
    $request.ContentType = 'application/json; charset=utf-8'

    if ($PSBoundParameters.ContainsKey('TimeoutSec') -and $null -ne $TimeoutSec) {
        $request.Timeout = [int]$TimeoutSec * 1000
        $request.ReadWriteTimeout = [int]$TimeoutSec * 1000
    }

    if ($null -ne $Headers) {
        foreach ($key in $Headers.Keys) {
            switch -Regex ($key) {
                '^Accept$' { $request.Accept = [string]$Headers[$key]; continue }
                '^Content-Type$' { $request.ContentType = [string]$Headers[$key]; continue }
                '^User-Agent$' { $request.UserAgent = [string]$Headers[$key]; continue }
                default { $request.Headers[$key] = [string]$Headers[$key] }
            }
        }
    }

    if ($Method -eq 'POST' -and $null -ne $Body) {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Body)
        $request.ContentLength = $bytes.Length
        $requestStream = $request.GetRequestStream()
        try {
            $requestStream.Write($bytes, 0, $bytes.Length)
        }
        finally {
            $requestStream.Dispose()
        }
    }
    else {
        $request.ContentLength = 0
    }

    try {
        $response = [System.Net.HttpWebResponse]$request.GetResponse()
        try {
            $reader = [System.IO.StreamReader]::new($response.GetResponseStream())
            try { $content = $reader.ReadToEnd() } finally { $reader.Dispose() }
        }
        finally {
            $response.Close()
        }

        $data = $null
        if (-not [string]::IsNullOrWhiteSpace($content)) {
            try { $data = $content | ConvertFrom-Json } catch { $data = $null }
        }

        return [pscustomobject]@{
            Success      = $true
            StatusCode   = [int]$response.StatusCode
            Body         = $content
            Data         = $data
            ErrorMessage = $null
            Transport    = 'HttpWebRequest'
        }
    }
    catch [System.Net.WebException] {
        $statusCode = $null
        $errorBody = $null
        $errorMessage = $_.Exception.Message
        if ($null -ne $_.Exception.Response) {
            try { $statusCode = [int]([System.Net.HttpWebResponse]$_.Exception.Response).StatusCode } catch { $statusCode = $null }
            try {
                $reader = [System.IO.StreamReader]::new($_.Exception.Response.GetResponseStream())
                try { $errorBody = $reader.ReadToEnd() } finally { $reader.Dispose() }
            }
            catch {
            }
        }
        $data = $null
        if (-not [string]::IsNullOrWhiteSpace($errorBody)) {
            try { $data = $errorBody | ConvertFrom-Json } catch { $data = $null }
        }
        return [pscustomobject]@{
            Success      = $false
            StatusCode   = $statusCode
            Body         = $errorBody
            Data         = $data
            ErrorMessage = $errorMessage
            Transport    = 'HttpWebRequest'
        }
    }
}

function Invoke-WebRequestSafe {
    param(
        [Parameter(Mandatory)]
        [ValidateSet('GET', 'POST')]
        [string]$Method,
        [Parameter(Mandatory)]
        [string]$Uri,
        [hashtable]$Headers,
        [string]$Body,
        [Nullable[int]]$TimeoutSec,
        [switch]$SkipTlsValidation
    )

    $originalCallback = $null
    $originalProtocol = $null
    $originalExpect100Continue = $null
    $originalRevocation = $null

    try {
        try {
            $originalProtocol = [System.Net.ServicePointManager]::SecurityProtocol
            [System.Net.ServicePointManager]::SecurityProtocol = Get-CompatibleSecurityProtocol
        }
        catch {
        }

        try {
            $originalExpect100Continue = [System.Net.ServicePointManager]::Expect100Continue
            [System.Net.ServicePointManager]::Expect100Continue = $false
        }
        catch {
        }

        try {
            $originalRevocation = [System.Net.ServicePointManager]::CheckCertificateRevocationList
            [System.Net.ServicePointManager]::CheckCertificateRevocationList = $false
        }
        catch {
        }

        if ($SkipTlsValidation) {
            try {
                $originalCallback = [System.Net.ServicePointManager]::ServerCertificateValidationCallback
                [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
            }
            catch {
            }
        }

        $invokeArgs = @{
            Method          = $Method
            Uri             = $Uri
            Headers         = $Headers
            UseBasicParsing = $true
            DisableKeepAlive= $true
        }

        if ($PSBoundParameters.ContainsKey('Body') -and $null -ne $Body) {
            $invokeArgs.Body = $Body
            $invokeArgs.ContentType = 'application/json; charset=utf-8'
        }

        if ($PSBoundParameters.ContainsKey('TimeoutSec') -and $null -ne $TimeoutSec) {
            $invokeArgs.TimeoutSec = $TimeoutSec
        }

        try {
            $response = Invoke-WebRequest @invokeArgs
            $content = $null
            try { $content = $response.Content } catch { $content = $null }

            $data = $null
            if (-not [string]::IsNullOrWhiteSpace($content)) {
                try { $data = $content | ConvertFrom-Json } catch { $data = $null }
            }

            return [pscustomobject]@{
                Success      = $true
                StatusCode   = [int]$response.StatusCode
                Body         = $content
                Data         = $data
                ErrorMessage = $null
                Transport    = 'Invoke-WebRequest'
            }
        }
        catch {
            $primaryError = $_.Exception.Message
            $shouldFallback = $true
            if ($_.Exception.PSObject.Properties.Name -contains 'Response' -and $null -ne $_.Exception.Response) {
                $shouldFallback = $false
            }

            if ($shouldFallback) {
                return Invoke-HttpWebRequestCompat -Method $Method -Uri $Uri -Headers $Headers -Body $Body -TimeoutSec $TimeoutSec -SkipTlsValidation:$SkipTlsValidation
            }

            throw
        }
    }
    catch {
        $statusCode = $null
        $errorBody = $null
        $errorMessage = $_.Exception.Message

        if ($_.Exception.PSObject.Properties.Name -contains 'Response' -and $null -ne $_.Exception.Response) {
            try { $statusCode = [int]$_.Exception.Response.StatusCode } catch { $statusCode = $null }
            try {
                $stream = $_.Exception.Response.GetResponseStream()
                if ($stream) {
                    $reader = [System.IO.StreamReader]::new($stream)
                    try { $errorBody = $reader.ReadToEnd() } finally { $reader.Dispose() }
                }
            }
            catch {
            }
        }

        $data = $null
        if (-not [string]::IsNullOrWhiteSpace($errorBody)) {
            try { $data = $errorBody | ConvertFrom-Json } catch { $data = $null }
        }

        return [pscustomobject]@{
            Success      = $false
            StatusCode   = $statusCode
            Body         = $errorBody
            Data         = $data
            ErrorMessage = $errorMessage
            Transport    = 'Invoke-WebRequest'
        }
    }
    finally {
        if ($SkipTlsValidation) {
            try { [System.Net.ServicePointManager]::ServerCertificateValidationCallback = $originalCallback } catch {}
        }

        if ($null -ne $originalProtocol) {
            try { [System.Net.ServicePointManager]::SecurityProtocol = $originalProtocol } catch {}
        }

        if ($null -ne $originalExpect100Continue) {
            try { [System.Net.ServicePointManager]::Expect100Continue = $originalExpect100Continue } catch {}
        }

        if ($null -ne $originalRevocation) {
            try { [System.Net.ServicePointManager]::CheckCertificateRevocationList = $originalRevocation } catch {}
        }
    }
}

function Get-CloudSEKAlertsFromResponse {
    param(
        $ResponseObject
    )

    if ($null -eq $ResponseObject) {
        return ,@()
    }

    if ($ResponseObject.PSObject.Properties.Name -contains 'data') {
        if ($null -eq $ResponseObject.data) {
            return ,@()
        }

        return @($ResponseObject.data | Where-Object { $null -ne $_ })
    }

    return @($ResponseObject | Where-Object { $null -ne $_ })
}

function Get-CloudSEKAlertSourceRef {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    $eventId = Get-SafeString (Get-SafeValue { $Alert.event_details.event_id })
    $incidentDisplayId = Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.incident_id })
    $incidentUuid = Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.id })

    $candidate = Get-FirstNonEmpty $eventId $incidentDisplayId $incidentUuid
    if (-not [string]::IsNullOrWhiteSpace($candidate)) {
        return $candidate
    }

    $fallbackJson = ConvertTo-JsonSafe -InputObject $Alert -Compress
    return ('cloudsek-' + (Get-StringSha256 -Text $fallbackJson).Substring(0, 16))
}

function Get-CloudSEKSourceLink {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    return (Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.event_url })) `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.incident_url })) `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.source_url }))
    )
}

function Get-CloudSEKPriorityText {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    return (Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.priority })) `
        (Get-SafeString (Get-SafeValue { $Alert.priority })) `
        'Unknown'
    )
}

function Get-CloudSEKIncidentStatusText {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    return (Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.incident_status })) `
        (Get-SafeString (Get-SafeValue { $Alert.status })) `
        'open'
    )
}

function Get-CloudSEKMagnitudeText {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    return (Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.threat_magnitude })) `
        (Get-CloudSEKPriorityText -Alert $Alert) `
        'Unknown'
    )
}

function Get-CloudSEKAlertTitle {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    $subModule = Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.sub_module })) `
        (Get-SafeString (Get-SafeValue { $Alert.module_name })) `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.source_group })) `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.source_name })) `
        'Alert'

    $ref = Get-CloudSEKAlertSourceRef -Alert $Alert
    $magnitude = Get-CloudSEKMagnitudeText -Alert $Alert

    $title = ('CloudSEK - {0} | {1} | {2}' -f $subModule, $ref, $magnitude)
    $title = $title -replace '[\r\n]+', ' '
    $title = $title -replace '\s{2,}', ' '
    return (Limit-Text -Text $title.Trim() -MaxLength 120)
}

function Get-CloudSEKAlertSummaryText {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    return (Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.event_summary })) `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.description })) `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.matched_alert_rule })) `
        'Sem resumo disponibilizado pela CloudSEK.'
    )
}

function Get-CloudSEKAlertDescription {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    $moduleName = Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.module_name })) `
        'Not Available'
    $subModule = Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.sub_module })) `
        $moduleName `
        'Not Available'
    $priority = Get-CloudSEKPriorityText -Alert $Alert
    $status = Get-CloudSEKIncidentStatusText -Alert $Alert
    $incidentId = Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.incident_id })) `
        'Not Available'
    $eventId = Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.event_id })) `
        'Not Available'
    $summary = Limit-Text -Text (Get-CloudSEKAlertSummaryText -Alert $Alert) -MaxLength 1000
    $primaryContext = Get-FirstNonEmpty `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.subdomain })) `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.source_name })) `
        (Get-SafeString (Get-SafeValue { $Alert.event_details.source_group })) `
        'Not Available'

    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add('Resumo Executivo')
    $lines.Add('Origem: CloudSEK')
    $lines.Add(('Modulo: {0}' -f $moduleName))
    $lines.Add(('Submodulo: {0}' -f $subModule))
    $lines.Add(('Prioridade: {0}' -f $priority))
    $lines.Add(('Status de origem: {0}' -f $status))
    $lines.Add(('Ativo principal: {0}' -f $primaryContext))
    $lines.Add(('Incident ID: {0}' -f $incidentId))
    $lines.Add(('Event ID: {0}' -f $eventId))
    $lines.Add('')
    $lines.Add('Resumo para triagem')
    $lines.Add($summary)

    return ($lines -join [Environment]::NewLine)
}

function Get-CloudSEKTags {
    param(
        [Parameter(Mandatory)]
        $Alert
    )

    $tags = [System.Collections.Generic.List[string]]::new()
    $tags.Add('cloudsek')

    foreach ($value in @(
        (Get-SafeString (Get-SafeValue { $Alert.module_name })),
        (Get-SafeString (Get-SafeValue { $Alert.sub_module })),
        (Get-SafeString (Get-SafeValue { $Alert.event_details.source_group })),
        (Get-SafeString (Get-SafeValue { $Alert.event_details.source_name })),
        (Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.priority }))
    )) {
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            $tags.Add(($value -replace '[,;]', ' '))
        }
    }

    $distinct = $tags | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { $_.Trim() } | Select-Object -Unique
    return ($distinct -join ',')
}

function Get-IrisSeverityId {
    param(
        [Parameter(Mandatory)]
        [string]$PriorityText,
        [Parameter(Mandatory)]
        $Config
    )

    switch ($PriorityText.ToUpperInvariant()) {
        'P0' { return [int]$Config.IRIS.SeverityMap.P0 }
        'P1' { return [int]$Config.IRIS.SeverityMap.P1 }
        'P2' { return [int]$Config.IRIS.SeverityMap.P2 }
        default { return [int]$Config.IRIS.SeverityMap.Default }
    }
}

function Get-IrisStatusId {
    param(
        [Parameter(Mandatory)]
        [string]$StatusText,
        [Parameter(Mandatory)]
        $Config
    )

    switch ($StatusText.ToLowerInvariant()) {
        'open'                  { return [int]$Config.IRIS.StatusMap.open }
        'acknowledged'          { return [int]$Config.IRIS.StatusMap.acknowledged }
        'in_progress'           { return [int]$Config.IRIS.StatusMap.in_progress }
        'reopened'              { return [int]$Config.IRIS.StatusMap.reopened }
        'closed_false_positive' { return [int]$Config.IRIS.StatusMap.closed }
        'closed_resolved'       { return [int]$Config.IRIS.StatusMap.closed }
        'closed_irrelevant'     { return [int]$Config.IRIS.StatusMap.closed }
        'closed_informational'  { return [int]$Config.IRIS.StatusMap.closed }
        default                 { return [int]$Config.IRIS.StatusMap.default }
    }
}

function New-MinimalAlertSourceContentString {
    param(
        [Parameter(Mandatory)]
        $Alert,
        [Parameter(Mandatory)]
        [string]$SourceRef
    )

    $content = [ordered]@{
        vendor          = 'CloudSEK'
        source_ref      = $SourceRef
        module_name     = Get-SafeString (Get-SafeValue { $Alert.module_name })
        sub_module      = Get-SafeString (Get-SafeValue { $Alert.sub_module })
        incident_id     = Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.incident_id })
        event_id        = Get-SafeString (Get-SafeValue { $Alert.event_details.event_id })
        priority        = Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.priority })
        incident_status = Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.incident_status })
        source_group    = Get-SafeString (Get-SafeValue { $Alert.event_details.source_group })
        source_name     = Get-SafeString (Get-SafeValue { $Alert.event_details.source_name })
        event_url       = Get-SafeString (Get-SafeValue { $Alert.event_details.event_url })
        source_url      = Get-SafeString (Get-SafeValue { $Alert.event_details.source_url })
        summary         = Limit-Text -Text (Get-CloudSEKAlertSummaryText -Alert $Alert) -MaxLength 400
    }

    $clean = [ordered]@{}
    foreach ($property in $content.GetEnumerator()) {
        if ($null -eq $property.Value) { continue }
        if ($property.Value -is [string] -and [string]::IsNullOrWhiteSpace($property.Value)) { continue }
        $clean[$property.Key] = $property.Value
    }

    return (ConvertTo-JsonSafe -InputObject $clean -Compress)
}

function New-IrisRequestPayload {
    param(
        [Parameter(Mandatory)]
        $Alert,
        [Parameter(Mandatory)]
        $Config
    )

    $sourceRef = Get-CloudSEKAlertSourceRef -Alert $Alert
    $priority = Get-CloudSEKPriorityText -Alert $Alert
    $status = Get-CloudSEKIncidentStatusText -Alert $Alert

    return [ordered]@{
        alert_title             = Get-CloudSEKAlertTitle -Alert $Alert
        alert_description       = Get-CloudSEKAlertDescription -Alert $Alert
        alert_source            = 'CloudSEK'
        alert_source_ref        = $sourceRef
        alert_source_link       = Get-CloudSEKSourceLink -Alert $Alert
        alert_source_content    = New-MinimalAlertSourceContentString -Alert $Alert -SourceRef $sourceRef
        alert_severity_id       = Get-IrisSeverityId -PriorityText $priority -Config $Config
        alert_status_id         = Get-IrisStatusId -StatusText $status -Config $Config
        alert_customer_id       = [int]$Config.IRIS.AlertCustomerId
        alert_classification_id = [int]$Config.IRIS.AlertClassificationId
    }
}

function New-NormalizedArtifact {
    param(
        [Parameter(Mandatory)]
        $Alert,
        [Parameter(Mandatory)]
        $Config,
        [Parameter(Mandatory)]
        [string]$RawPath
    )

    $sourceRef = Get-CloudSEKAlertSourceRef -Alert $Alert
    $requestPayload = New-IrisRequestPayload -Alert $Alert -Config $Config

    return [ordered]@{
        normalization_version = 'cloudsek-iris-final-v1'
        normalized_at         = Get-UtcNowString
        source_ref            = $sourceRef
        raw_path              = $RawPath
        cloudsek = [ordered]@{
            module_name     = Get-SafeString (Get-SafeValue { $Alert.module_name })
            sub_module      = Get-SafeString (Get-SafeValue { $Alert.sub_module })
            priority        = Get-CloudSEKPriorityText -Alert $Alert
            incident_status = Get-CloudSEKIncidentStatusText -Alert $Alert
            incident_id     = Get-SafeString (Get-SafeValue { $Alert.event_details.incident_details.incident_id })
            event_id        = Get-SafeString (Get-SafeValue { $Alert.event_details.event_id })
            source_link     = Get-CloudSEKSourceLink -Alert $Alert
            tags            = Get-CloudSEKTags -Alert $Alert
        }
        iris_request = $requestPayload
    }
}

function Get-IrisCreateHeaders {
    param(
        [Parameter(Mandatory)]
        [string]$ApiKey
    )

    return @{
        'Authorization' = "Bearer $ApiKey"
        'Accept'        = 'application/json'
        'Content-Type'  = 'application/json'
    }
}

function Get-IrisAlertIdFromResponse {
    param(
        $ResponseData
    )

    if ($null -eq $ResponseData) {
        return $null
    }

    $candidates = @(
        (Get-SafeValue { $ResponseData.alert_id }),
        (Get-SafeValue { $ResponseData.data.alert_id }),
        (Get-SafeValue { $ResponseData.data.id }),
        (Get-SafeValue { $ResponseData.id })
    )

    foreach ($candidate in $candidates) {
        if ($null -eq $candidate) { continue }
        try {
            return [int]$candidate
        }
        catch {
        }
    }

    return $null
}

function Save-FailureRecord {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath,
        [Parameter(Mandatory)]
        [string]$RawFilePath,
        [Parameter(Mandatory)]
        [string]$SourceRef,
        [Parameter(Mandatory)]
        [string]$Reason,
        [Parameter(Mandatory)]
        [string]$Detail,
        [Nullable[int]]$HttpStatus,
        [string]$RequestFile,
        [string]$ResponseFile
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $failureRecord = [ordered]@{
        failed_at       = Get-UtcNowString
        raw_file_name   = [System.IO.Path]::GetFileName($RawFilePath)
        raw_file_path   = $RawFilePath
        source_ref      = $SourceRef
        reason          = $Reason
        detail          = $Detail
        http_status     = $HttpStatus
        request_file    = $RequestFile
        response_file   = $ResponseFile
    }

    $sidecarFile = Join-Path -Path $paths.Failed -ChildPath (([System.IO.Path]::GetFileNameWithoutExtension($RawFilePath)) + '.failure.json')
    Save-JsonFile -Path $sidecarFile -Object $failureRecord

    $jsonlLine = ConvertTo-JsonSafe -InputObject $failureRecord -Compress
    Add-Content -Path $paths.FailureLogFile -Value $jsonlLine -Encoding UTF8

    return $sidecarFile
}

function Move-RawToFailed {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath,
        [Parameter(Mandatory)]
        [string]$RawPath
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $destination = Join-Path -Path $paths.Failed -ChildPath ([System.IO.Path]::GetFileName($RawPath))

    if ([System.IO.Path]::GetFullPath($RawPath) -eq [System.IO.Path]::GetFullPath($destination)) {
        return $destination
    }

    Move-Item -LiteralPath $RawPath -Destination $destination -Force
    return $destination
}

function Remove-FailureSidecarIfPresent {
    param(
        [Parameter(Mandatory)]
        [string]$RawPath
    )

    $directory = Split-Path -Path $RawPath -Parent
    $sidecar = Join-Path -Path $directory -ChildPath (([System.IO.Path]::GetFileNameWithoutExtension($RawPath)) + '.failure.json')
    if (Test-Path -LiteralPath $sidecar) {
        Remove-Item -LiteralPath $sidecar -Force
    }
}

function Save-RawAlertToIncoming {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath,
        [Parameter(Mandatory)]
        $Alert
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $sourceRef = Get-CloudSEKAlertSourceRef -Alert $Alert
    $fileName = ('{0}__{1}.raw.json' -f (Get-NowFileStamp), (Get-SafeFileToken -Value $sourceRef))
    $rawPath = Join-Path -Path $paths.Incoming -ChildPath $fileName
    Save-JsonFile -Path $rawPath -Object $Alert
    return [ordered]@{
        source_ref = $sourceRef
        raw_path   = $rawPath
    }
}

function Receive-NewCloudSEKAlerts {
    param(
        [Parameter(Mandatory)]
        $Config
    )

    $rootPath = [string]$Config.RootPath
    $headers = New-CloudSEKHeaders -ApiKey ([string]$Config.CloudSEK.ApiKey)
    $uri = New-CloudSEKAlertsUri -BaseUrl ([string]$Config.CloudSEK.BaseUrl) -Limit ([int]$Config.CloudSEK.Limit) -Page ([int]$Config.CloudSEK.Page) -ModuleName @($Config.CloudSEK.ModuleName)

    Write-OperationalLog -RootPath $rootPath -Level 'INFO' -Message ('Consultando CloudSEK em {0}' -f $uri)
    $result = Invoke-WebRequestSafe -Method GET -Uri $uri -Headers $headers -TimeoutSec ([int]$Config.IRIS.TimeoutSec)

    if (-not $result.Success) {
        Write-OperationalLog -RootPath $rootPath -Level 'ERROR' -Message 'Falha ao consultar a CloudSEK.' -Context ([ordered]@{ status = $result.StatusCode; detail = $result.ErrorMessage; response = $result.Body })
        throw ('Falha ao consultar a CloudSEK. HTTP={0}. {1}' -f $result.StatusCode, $result.ErrorMessage)
    }

    $alerts = @(Get-CloudSEKAlertsFromResponse -ResponseObject $result.Data)
    Write-OperationalLog -RootPath $rootPath -Level 'INFO' -Message ('CloudSEK retornou {0} alerta(s).' -f $alerts.Count)

    $saved = [System.Collections.Generic.List[object]]::new()
    foreach ($alert in $alerts) {
        $rawInfo = Save-RawAlertToIncoming -RootPath $rootPath -Alert $alert
        $saved.Add($rawInfo)
        Write-OperationalLog -RootPath $rootPath -Level 'INFO' -Message ('Bruto salvo em {0}' -f $rawInfo.raw_path) -Context ([ordered]@{ source_ref = $rawInfo.source_ref })

        $queue = Read-QueueState -RootPath $rootPath
        Update-QueueRecordStatus -QueueState $queue -SourceRef ([string]$rawInfo.source_ref) -Status 'captured_pending_send' -RawFile ([string]$rawInfo.raw_path) -Location 'incoming' -AttemptCount 0
        Save-QueueState -RootPath $rootPath -QueueState $queue
    }

    return @($saved)
}

function Get-RawFilesOrdered {
    param(
        [Parameter(Mandatory)]
        [string]$DirectoryPath
    )

    if (-not (Test-Path -LiteralPath $DirectoryPath)) {
        return ,@()
    }

    return @(Get-ChildItem -LiteralPath $DirectoryPath -Filter '*.raw.json' -File | Sort-Object LastWriteTimeUtc, Name)
}

function Process-RawAlertFile {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath,
        [Parameter(Mandatory)]
        [string]$RawPath,
        [Parameter(Mandatory)]
        [string]$Location,
        [Parameter(Mandatory)]
        $Config
    )

    $paths = Ensure-DirectoryStructure -RootPath $RootPath
    $queue = Read-QueueState -RootPath $RootPath
    $sent = Read-SentState -RootPath $RootPath

    $rawJson = [System.IO.File]::ReadAllText($RawPath, [System.Text.UTF8Encoding]::new($false))
    $alert = $rawJson | ConvertFrom-Json
    $sourceRef = Get-CloudSEKAlertSourceRef -Alert $alert
    $existingSent = Get-SentRecord -SentState $sent -SourceRef $sourceRef

    if ($null -ne $existingSent) {
        Update-QueueRecordStatus -QueueState $queue -SourceRef $sourceRef -Status 'skipped_already_sent' -RawFile $RawPath -Location $Location -LastErrorReason $null -LastErrorDetail 'Ignored because it is already recorded as sent in local state.'
        Save-QueueState -RootPath $RootPath -QueueState $queue
        Remove-FailureSidecarIfPresent -RawPath $RawPath
        Remove-Item -LiteralPath $RawPath -Force
        Write-OperationalLog -RootPath $RootPath -Level 'WARN' -Message ('Skipping {0}; alert_source_ref was already marked as sent.' -f $sourceRef)
        return
    }

    $attemptCount = 1
    foreach ($queueItem in @($queue.items)) {
        if ((Get-RecordSourceRef -Object $queueItem) -eq $sourceRef) {
            try { $attemptCount = [int]$queueItem.attempts + 1 } catch { $attemptCount = 1 }
            break
        }
    }

    Update-QueueRecordStatus -QueueState $queue -SourceRef $sourceRef -Status 'processing' -RawFile $RawPath -Location $Location -AttemptCount $attemptCount
    Save-QueueState -RootPath $RootPath -QueueState $queue

    $fileBase = [System.IO.Path]::GetFileNameWithoutExtension($RawPath)
    $normalizedPath = Join-Path -Path $paths.WorkNormalized -ChildPath ($fileBase + '.normalized.json')
    $requestPath = Join-Path -Path $paths.WorkRequests -ChildPath ($fileBase + '.iris-request.json')
    $responsePath = Join-Path -Path $paths.WorkResponses -ChildPath ($fileBase + '.iris-response.json')

    try {
        $normalized = New-NormalizedArtifact -Alert $alert -Config $Config -RawPath $RawPath
        Save-JsonFile -Path $normalizedPath -Object $normalized

        $requestBodyObject = $normalized.iris_request
        Save-JsonFile -Path $requestPath -Object $requestBodyObject
        $requestBodyJson = ConvertTo-JsonSafe -InputObject $requestBodyObject

        $irisHeaders = Get-IrisCreateHeaders -ApiKey ([string]$Config.IRIS.ApiKey)
        $irisResult = Invoke-WebRequestSafe -Method POST -Uri ([string]$Config.IRIS.CreateUri) -Headers $irisHeaders -Body $requestBodyJson -TimeoutSec ([int]$Config.IRIS.TimeoutSec) -SkipTlsValidation:([bool]$Config.IRIS.SkipTlsValidation)

        $responseArtifact = [ordered]@{
            processed_at     = Get-UtcNowString
            source_ref       = $sourceRef
            raw_file         = $RawPath
            normalized_file  = $normalizedPath
            request_file     = $requestPath
            http_status      = $irisResult.StatusCode
            success          = $irisResult.Success
            response_body    = $irisResult.Body
            response_data    = $irisResult.Data
            error_message    = $irisResult.ErrorMessage
        }
        Save-JsonFile -Path $responsePath -Object $responseArtifact

        $irisAlertId = $null
        if ($irisResult.Success -and $irisResult.StatusCode -eq 200) {
            $irisAlertId = Get-IrisAlertIdFromResponse -ResponseData $irisResult.Data
        }

        if ($irisResult.Success -and $irisResult.StatusCode -eq 200 -and $null -ne $irisAlertId) {
            $sentRecord = [ordered]@{
                source_ref      = $sourceRef
                sent_at         = Get-UtcNowString
                iris_alert_id   = $irisAlertId
                raw_file        = $RawPath
                normalized_file = $normalizedPath
                request_file    = $requestPath
                response_file   = $responsePath
            }
            Upsert-SentRecord -SentState $sent -Record $sentRecord
            Save-SentState -RootPath $RootPath -SentState $sent

            Update-QueueRecordStatus -QueueState $queue -SourceRef $sourceRef -Status 'sent_successfully' -RawFile $RawPath -Location $Location -NormalizedFile $normalizedPath -RequestFile $requestPath -ResponseFile $responsePath -AttemptCount $attemptCount -LastHttpStatus $irisResult.StatusCode -LastErrorReason $null -LastErrorDetail $null -IrisAlertId $irisAlertId
            Save-QueueState -RootPath $RootPath -QueueState $queue

            Remove-FailureSidecarIfPresent -RawPath $RawPath
            Remove-Item -LiteralPath $RawPath -Force
            Write-OperationalLog -RootPath $RootPath -Level 'INFO' -Message ('Alerta {0} criado no IRIS com sucesso. IRIS alert_id={1}' -f $sourceRef, $irisAlertId)
            return
        }

        $reason = if ($irisResult.StatusCode -eq 200 -and $null -eq $irisAlertId) {
            'http_200_without_confirmed_alert_id'
        }
        elseif ($null -eq $irisResult.StatusCode) {
            'transport_or_tls_error'
        }
        else {
            'iris_request_failed'
        }

        $detail = Get-FirstNonEmpty $irisResult.ErrorMessage $irisResult.Body 'Falha sem detalhe adicional.'
        $failedRawPath = Move-RawToFailed -RootPath $RootPath -RawPath $RawPath
        $null = Save-FailureRecord -RootPath $RootPath -RawFilePath $failedRawPath -SourceRef $sourceRef -Reason $reason -Detail $detail -HttpStatus $irisResult.StatusCode -RequestFile $requestPath -ResponseFile $responsePath

        Update-QueueRecordStatus -QueueState $queue -SourceRef $sourceRef -Status 'failed' -RawFile $failedRawPath -Location 'failed' -NormalizedFile $normalizedPath -RequestFile $requestPath -ResponseFile $responsePath -AttemptCount $attemptCount -LastHttpStatus $irisResult.StatusCode -LastErrorReason $reason -LastErrorDetail $detail -IrisAlertId $null
        Save-QueueState -RootPath $RootPath -QueueState $queue

        Write-OperationalLog -RootPath $RootPath -Level 'ERROR' -Message ('Falha ao enviar {0} ao IRIS. Item movido para failed.' -f $sourceRef) -Context ([ordered]@{ reason = $reason; status = $irisResult.StatusCode; detail = $detail })
    }
    catch {
        $detail = $_.Exception.Message
        $failedRawPath = Move-RawToFailed -RootPath $RootPath -RawPath $RawPath
        $null = Save-FailureRecord -RootPath $RootPath -RawFilePath $failedRawPath -SourceRef $sourceRef -Reason 'processing_exception' -Detail $detail -HttpStatus $null -RequestFile $requestPath -ResponseFile $responsePath

        Update-QueueRecordStatus -QueueState $queue -SourceRef $sourceRef -Status 'failed' -RawFile $failedRawPath -Location 'failed' -NormalizedFile $normalizedPath -RequestFile $requestPath -ResponseFile $responsePath -AttemptCount $attemptCount -LastHttpStatus $null -LastErrorReason 'processing_exception' -LastErrorDetail $detail -IrisAlertId $null
        Save-QueueState -RootPath $RootPath -QueueState $queue

        Write-OperationalLog -RootPath $RootPath -Level 'ERROR' -Message ('Exception while processing {0}. Item moved to failed.' -f $sourceRef) -Context ([ordered]@{ detail = $detail })
    }
}

function Process-IncomingQueue {
    param(
        [Parameter(Mandatory)]
        $Config
    )

    $paths = Ensure-DirectoryStructure -RootPath ([string]$Config.RootPath)
    $files = @(Get-RawFilesOrdered -DirectoryPath $paths.Incoming)
    Write-OperationalLog -RootPath ([string]$Config.RootPath) -Level 'INFO' -Message ('Incoming queue has {0} item(s).' -f $files.Count)

    foreach ($file in $files) {
        Process-RawAlertFile -RootPath ([string]$Config.RootPath) -RawPath $file.FullName -Location 'incoming' -Config $Config
    }
}

function Process-FailedQueue {
    param(
        [Parameter(Mandatory)]
        $Config
    )

    $paths = Ensure-DirectoryStructure -RootPath ([string]$Config.RootPath)
    $files = @(Get-RawFilesOrdered -DirectoryPath $paths.Failed)
    Write-OperationalLog -RootPath ([string]$Config.RootPath) -Level 'INFO' -Message ('Failed queue has {0} item(s).' -f $files.Count)

    foreach ($file in $files) {
        Process-RawAlertFile -RootPath ([string]$Config.RootPath) -RawPath $file.FullName -Location 'failed' -Config $Config
    }
}

function Invoke-Cycle {
    param(
        [Parameter(Mandatory)]
        $Config,
        [Parameter(Mandatory)]
        [string]$Mode
    )

    $rootPath = [string]$Config.RootPath
    Write-OperationalLog -RootPath $rootPath -Level 'INFO' -Message ('Cycle started in mode {0}.' -f $Mode)

    switch ($Mode) {
        'LivePoll' {
            $null = Receive-NewCloudSEKAlerts -Config $Config
            Process-IncomingQueue -Config $Config
        }
        'PendingOnly' {
            Process-IncomingQueue -Config $Config
        }
        'RetryFailed' {
            Process-FailedQueue -Config $Config
        }
        default {
            throw "Unsupported mode: $Mode"
        }
    }

    Write-OperationalLog -RootPath $rootPath -Level 'INFO' -Message ('Cycle finished in mode {0}.' -f $Mode)
}

function Test-ConfigReadyForOperationalRun {
    param(
        [Parameter(Mandatory)]
        [string]$ConfigPath
    )

    $config = Read-JsonFileOrDefault -Path $ConfigPath -DefaultObject $null
    if ($null -eq $config) {
        return $false
    }

    $checks = @(
        (Get-SafeValue { $config.RootPath }),
        (Get-SafeValue { $config.CloudSEK.ApiKey }),
        (Get-SafeValue { $config.IRIS.ApiKey }),
        (Get-SafeValue { $config.IRIS.CreateUri }),
        (Get-SafeValue { $config.IRIS.SeverityMap.P0 }),
        (Get-SafeValue { $config.IRIS.SeverityMap.P1 }),
        (Get-SafeValue { $config.IRIS.SeverityMap.P2 }),
        (Get-SafeValue { $config.IRIS.SeverityMap.Default }),
        (Get-SafeValue { $config.IRIS.StatusMap.open }),
        (Get-SafeValue { $config.IRIS.StatusMap.acknowledged }),
        (Get-SafeValue { $config.IRIS.StatusMap.in_progress }),
        (Get-SafeValue { $config.IRIS.StatusMap.reopened }),
        (Get-SafeValue { $config.IRIS.StatusMap.closed }),
        (Get-SafeValue { $config.IRIS.StatusMap.default })
    )

    foreach ($item in $checks) {
        if (Test-IsPlaceholderValue -Value $item) {
            return $false
        }
    }

    return $true
}

function Write-ConfigPendingMessage {
    param(
        [Parameter(Mandatory)]
        [string]$ConfigPath
    )

    Write-Host ("Config file '{0}' is not ready yet. Fill CloudSEK.ApiKey and IRIS.ApiKey. SeverityMap and StatusMap already use the validated mapping discovered in this environment. Re-run CloudSEK-Discover-IrisMappings.ps1 only if you want to revalidate or revise those IDs later." -f $ConfigPath) -ForegroundColor Yellow
}

try {
    if ($RunOnce -and (Test-Path -LiteralPath $ConfigPath) -and -not (Test-ConfigReadyForOperationalRun -ConfigPath $ConfigPath)) {
        Write-ConfigPendingMessage -ConfigPath $ConfigPath
        exit 0
    }

    $config = Load-ValidatedConfig -ConfigPath $ConfigPath
    if ($null -eq $config) {
        return
    }

    $null = Ensure-DirectoryStructure -RootPath ([string]$config.RootPath)

    Write-OperationalLog -RootPath ([string]$config.RootPath) -Level 'INFO' -Message 'Configuration loaded successfully.' -Context ([ordered]@{
        mode              = $Mode
        run_once          = [bool]$RunOnce
        root_path         = [string]$config.RootPath
        cloudsek_base_url = [string]$config.CloudSEK.BaseUrl
        cloudsek_limit    = [int]$config.CloudSEK.Limit
        interval_seconds  = [int]$config.CloudSEK.IntervalSeconds
        iris_create_uri   = [string]$config.IRIS.CreateUri
        iris_customer_id  = [int]$config.IRIS.AlertCustomerId
        iris_class_id     = [int]$config.IRIS.AlertClassificationId
    })

    do {
        Invoke-Cycle -Config $config -Mode $Mode
        if ($RunOnce) { break }
        Start-Sleep -Seconds ([int]$config.CloudSEK.IntervalSeconds)
    }
    while ($true)
}
catch {
    $fallbackRoot = 'C:\CloudSEK'
    try {
        $fallbackRaw = Read-JsonFileOrDefault -Path $ConfigPath -DefaultObject $null
        if ($null -ne $fallbackRaw -and $fallbackRaw.PSObject.Properties.Name -contains 'RootPath' -and -not [string]::IsNullOrWhiteSpace([string]$fallbackRaw.RootPath)) {
            $fallbackRoot = [string]$fallbackRaw.RootPath
        }
    }
    catch {
    }

    try {
        Write-OperationalLog -RootPath $fallbackRoot -Level 'ERROR' -Message 'Execution ended with error.' -Context ([ordered]@{ detail = $_.Exception.Message })
    }
    catch {
    }

    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}
