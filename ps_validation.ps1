param (
    [ValidateSet('Move', 'Copy')]
    [string]$Action = 'Move'
)

# ----------------------------
# Configuration
# ----------------------------

$csvFolder = "\\ysvvstore11\FS_YSL_YFB4-tijdelijk\Albert\Infectie preventie"
$sourceFolder = "\\ysvvstore11\FS_YSL_YFB4-nietmedisch\FS_YSL_YFB4-nietmedisch\Hans\Personeelsdossier"
$destinationFolder = "\\ysvvstore11\FS_YSL_YFB4-medisch\FS_YSL_YFB4-medisch\Infectie Preventie\Onbekende Medewerkers"
$csvFileName        = "Infectie Preventie - Onbekende Medewerker - VD - vaccienatie documenten.csv"

# Set the CSV delimiter.
# In Dutch CSV files this is often ';'
$csvDelimiter       = ';'

# Try to auto-detect delimiter from the header if possible.
# Falls back to $csvDelimiter when detection is inconclusive.
$autoDetectDelimiter = $true

# Set the CSV column that contains the file names.
# Leave empty ('') to automatically use the first column.
$fileNameColumn     = 'file'

function Normalize-CsvHeaderName {
    param (
        [Parameter(Mandatory = $true)]
        [string]$HeaderName
    )

    # Remove UTF-8 BOM and surrounding spaces/quotes from header names.
    return ($HeaderName -replace '^\uFEFF', '').Trim().Trim('"')
}

function Resolve-FileNameFromCsvValue {
    param (
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [AllowNull()]
        [string]$Value
    )

    $trimmedValue = $Value.Trim().Trim('"')

    if ([string]::IsNullOrWhiteSpace($trimmedValue)) {
        return ''
    }

    # Convert to only the file name so CSV values can contain relative paths or UNC/full paths.
    $fileName = [System.IO.Path]::GetFileName($trimmedValue)

    return $fileName.Trim()
}

function Get-CsvDelimiter {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$FallbackDelimiter
    )

    $firstLine = Get-Content -LiteralPath $Path -TotalCount 1 -ErrorAction Stop

    if ([string]::IsNullOrWhiteSpace($firstLine)) {
        return $FallbackDelimiter
    }

    $candidates = @(';', ',', "`t", '|')
    $bestDelimiter = $FallbackDelimiter
    $bestScore = -1

    foreach ($candidate in $candidates) {
        $score = ($firstLine.ToCharArray() | Where-Object { $_ -eq $candidate }).Count
        if ($score -gt $bestScore) {
            $bestScore = $score
            $bestDelimiter = $candidate
        }
    }

    if ($bestScore -le 0) {
        return $FallbackDelimiter
    }

    return $bestDelimiter
}


function Get-FilePathsRecursiveSafe {
    param (
        [Parameter(Mandatory = $true)]
        [string]$RootPath
    )

    $pendingDirectories = New-Object System.Collections.Generic.Stack[string]
    $pendingDirectories.Push($RootPath)

    while ($pendingDirectories.Count -gt 0) {
        $currentDirectory = $pendingDirectories.Pop()

        try {
            foreach ($filePath in [System.IO.Directory]::EnumerateFiles($currentDirectory, '*', [System.IO.SearchOption]::TopDirectoryOnly)) {
                $filePath
            }
        }
        catch {
            # Ignore inaccessible directories/files and continue scanning.
        }

        try {
            foreach ($subDirectory in [System.IO.Directory]::EnumerateDirectories($currentDirectory, '*', [System.IO.SearchOption]::TopDirectoryOnly)) {
                $pendingDirectories.Push($subDirectory)
            }
        }
        catch {
            # Ignore inaccessible directories and continue scanning.
        }
    }
}

function New-DestinationFileNameIndex {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Folder
    )

    $existingNames = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

    try {
        foreach ($existingPath in [System.IO.Directory]::EnumerateFiles($Folder, '*', [System.IO.SearchOption]::TopDirectoryOnly)) {
            [void]$existingNames.Add([System.IO.Path]::GetFileName($existingPath))
        }
    }
    catch {
        # If this fails we continue with an empty in-memory index.
    }

    # Return as a single scalar object.
    # -NoEnumerate prevents HashSet values from being expanded into the pipeline,
    # which can otherwise result in $null/empty-collection binding issues.
    Write-Output -NoEnumerate $existingNames
}

function Get-UniqueDestinationPath {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Folder,

        [Parameter(Mandatory = $true)]
        [string]$FileName,

        [Parameter(Mandatory = $true)]
        [System.Collections.Generic.HashSet[string]]$ExistingNames,

        [Parameter(Mandatory = $true)]
        [hashtable]$NextSuffixByKey
    )

    if ($ExistingNames.Add($FileName)) {
        return Join-Path -Path $Folder -ChildPath $FileName
    }

    $baseName   = [System.IO.Path]::GetFileNameWithoutExtension($FileName)
    $extension  = [System.IO.Path]::GetExtension($FileName)
    $suffixKey  = "$($baseName.ToLowerInvariant())|$($extension.ToLowerInvariant())"
    $counter    = if ($NextSuffixByKey.ContainsKey($suffixKey)) { [int]$NextSuffixByKey[$suffixKey] } else { 1 }
    $candidateName = ''

    while ($true) {
        $candidateName = "{0} ({1}){2}" -f $baseName, $counter, $extension
        if ($ExistingNames.Add($candidateName)) {
            break
        }
        $counter++
    }

    $NextSuffixByKey[$suffixKey] = $counter + 1
    return Join-Path -Path $Folder -ChildPath $candidateName
}

# ----------------------------
# Build paths and validate
# ----------------------------

$csvPath = Join-Path -Path $csvFolder -ChildPath $csvFileName

if (-not (Test-Path -LiteralPath $csvPath)) {
    throw "CSV file not found: $csvPath"
}

if (-not (Test-Path -LiteralPath $sourceFolder)) {
    throw "Source folder not found: $sourceFolder"
}

if (-not (Test-Path -LiteralPath $destinationFolder)) {
    New-Item -ItemType Directory -Path $destinationFolder -Force | Out-Null
}

# ----------------------------
# Import CSV
# ----------------------------

$effectiveDelimiter = if ($autoDetectDelimiter) {
    Get-CsvDelimiter -Path $csvPath -FallbackDelimiter $csvDelimiter
}
else {
    $csvDelimiter
}

$rows = Import-Csv -LiteralPath $csvPath -Delimiter $effectiveDelimiter

if (-not $rows -or $rows.Count -eq 0) {
    throw "The CSV file is empty."
}

# Automatically use the first column if no column name is specified
if ([string]::IsNullOrWhiteSpace($fileNameColumn)) {
    $fileNameColumn = $rows[0].PSObject.Properties.Name[0]
}

$headerMap = @{}
foreach ($header in $rows[0].PSObject.Properties.Name) {
    $normalizedHeader = Normalize-CsvHeaderName -HeaderName $header
    if (-not $headerMap.ContainsKey($normalizedHeader)) {
        $headerMap[$normalizedHeader] = $header
    }
}

$normalizedRequestedColumn = Normalize-CsvHeaderName -HeaderName $fileNameColumn

if (-not $headerMap.ContainsKey($normalizedRequestedColumn)) {
    $availableColumns = $rows[0].PSObject.Properties.Name -join ', '
    throw "Column '$fileNameColumn' was not found in the CSV. Available columns: $availableColumns. Delimiter used: '$effectiveDelimiter'."
}

$resolvedFileNameColumn = $headerMap[$normalizedRequestedColumn]

# Extract file names from the CSV.
# Wrap in @(...) so a single result stays an array and is not treated as a string.
$csvFileNames = @($rows |
    ForEach-Object { Resolve-FileNameFromCsvValue -Value ([string]($_.$resolvedFileNameColumn)) } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

$totalCsvRows            = $rows.Count
$totalCsvFileNames       = $csvFileNames.Count
$uniqueCsvFileNames      = @($csvFileNames | Sort-Object -Unique)
$totalUniqueCsvFileNames = $uniqueCsvFileNames.Count

# ----------------------------
# Prepare lookup set (optimized for very large source trees)
# ----------------------------

$targetFileNames = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($fileName in $uniqueCsvFileNames) {
    [void]$targetFileNames.Add($fileName)
}

# ----------------------------
# Scan source files (streaming)
# ----------------------------

Write-Host ""
Write-Host "Scanning source folder files (streaming, optimized for very large file counts)..." -ForegroundColor Cyan

$fileIndex = @{}
$totalSourceFiles = 0
$scanCheckpoint = [System.Diagnostics.Stopwatch]::StartNew()

$supportsEnumerationOptions = $null -ne ([type]::GetType('System.IO.EnumerationOptions', $false))

$filePathEnumerator = if ($supportsEnumerationOptions) {
    $enumerationOptions = [System.IO.EnumerationOptions]::new()
    $enumerationOptions.RecurseSubdirectories = $true
    $enumerationOptions.IgnoreInaccessible = $true
    $enumerationOptions.ReturnSpecialDirectories = $false

    [System.IO.Directory]::EnumerateFiles($sourceFolder, '*', $enumerationOptions)
}
else {
    Write-Host "System.IO.EnumerationOptions is not available. Using compatibility scan mode for older PowerShell/.NET versions." -ForegroundColor Yellow
    Get-FilePathsRecursiveSafe -RootPath $sourceFolder
}

foreach ($fullPath in $filePathEnumerator) {
    $totalSourceFiles++

    $name = [System.IO.Path]::GetFileName($fullPath)

    if ($targetFileNames.Contains($name)) {
        if (-not $fileIndex.ContainsKey($name)) {
            $fileIndex[$name] = New-Object System.Collections.Generic.List[string]
        }

        $fileIndex[$name].Add($fullPath)
    }

    if ($scanCheckpoint.Elapsed.TotalSeconds -ge 2) {
        $foundUnique = $fileIndex.Keys.Count
        Write-Progress `
            -Id 1 `
            -Activity "Scanning source files" `
            -Status "Scanned: $totalSourceFiles | Matched unique names: $foundUnique of $totalUniqueCsvFileNames"

        $scanCheckpoint.Restart()
    }
}

Write-Progress -Id 1 -Activity "Scanning source files" -Completed

# ----------------------------
# Process CSV file names
# ----------------------------

Write-Host ""
Write-Host "Processing CSV file names..." -ForegroundColor Cyan

$matchedCsvNames      = 0
$notFoundCsvNames     = 0
$processedFilesCount  = 0
$multiMatchNameCount  = 0
$errorsCount          = 0

$actionVerbPastTense = if ($Action -eq 'Copy') { 'Copied' } else { 'Moved' }
$actionVerbPresent   = if ($Action -eq 'Copy') { 'copying' } else { 'moving' }
$logFileName         = if ($Action -eq 'Copy') { 'copy-log.csv' } else { 'move-log.csv' }

$log = New-Object System.Collections.Generic.List[object]
$processStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$progressCheckpoint = [System.Diagnostics.Stopwatch]::StartNew()

$destinationNamesIndex = New-DestinationFileNameIndex -Folder $destinationFolder
$destinationNamesIndex = if ($destinationNamesIndex -is [System.Collections.Generic.HashSet[string]]) {
    $destinationNamesIndex
}
else {
    [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
}
$destinationSuffixIndex = @{}

for ($i = 0; $i -lt $totalUniqueCsvFileNames; $i++) {
    $fileName = $uniqueCsvFileNames[$i]

    $processed = $i + 1
    $percent   = [int](($processed / [Math]::Max($totalUniqueCsvFileNames, 1)) * 100)

    $elapsedSeconds = [Math]::Max($processStopwatch.Elapsed.TotalSeconds, 1)
    $ratePerSecond  = $processed / $elapsedSeconds
    $remainingItems = $totalUniqueCsvFileNames - $processed
    $secondsLeft    = if ($ratePerSecond -gt 0) { [int]($remainingItems / $ratePerSecond) } else { -1 }

    if ($progressCheckpoint.Elapsed.TotalMilliseconds -ge 500 -or $processed -eq 1 -or $processed -eq $totalUniqueCsvFileNames) {
        Write-Progress `
            -Id 2 `
            -Activity "Looking up and $actionVerbPresent files" `
            -Status "$processed of $totalUniqueCsvFileNames : $fileName" `
            -PercentComplete $percent `
            -SecondsRemaining $secondsLeft

        $progressCheckpoint.Restart()
    }

    if ($fileIndex.ContainsKey($fileName)) {
        $matches = $fileIndex[$fileName]
        $matchedCsvNames++

        $log.Add([PSCustomObject]@{
            FileName         = $fileName
            Status           = "Found"
            SourcePath       = $null
            DestinationPath  = $null
            Message          = "File name found in source folder ($($matches.Count) match(es))"
        })

        if ($matches.Count -gt 1) {
            $multiMatchNameCount++
        }

        foreach ($matchFullPath in $matches) {
            try {
                $destinationPath = Get-UniqueDestinationPath `
                    -Folder $destinationFolder `
                    -FileName $fileName `
                    -ExistingNames $destinationNamesIndex `
                    -NextSuffixByKey $destinationSuffixIndex

                if ($Action -eq 'Copy') {
                    Copy-Item -LiteralPath $matchFullPath -Destination $destinationPath -Force
                }
                else {
                    Move-Item -LiteralPath $matchFullPath -Destination $destinationPath -Force
                }

                $processedFilesCount++

                $log.Add([PSCustomObject]@{
                    FileName         = $fileName
                    Status           = $actionVerbPastTense
                    SourcePath       = $matchFullPath
                    DestinationPath  = $destinationPath
                    Message          = "File $($actionVerbPastTense.ToLower()) successfully"
                })

            }
            catch {
                $errorsCount++

                $log.Add([PSCustomObject]@{
                    FileName         = $fileName
                    Status           = "Error"
                    SourcePath       = $matchFullPath
                    DestinationPath  = $null
                    Message          = $_.Exception.Message
                })

                Write-Warning "$($actionVerbPastTense.ToUpper()) FAILED: '$matchFullPath'. Error: $($_.Exception.Message)"
            }
        }
    }
    else {
        $notFoundCsvNames++

        $log.Add([PSCustomObject]@{
            FileName         = $fileName
            Status           = "NotFound"
            SourcePath       = $null
            DestinationPath  = $null
            Message          = "File name not found in source folder"
        })

        Write-Warning "NOT FOUND: '$fileName'"
    }
}

Write-Progress -Id 2 -Activity "Looking up and $actionVerbPresent files" -Completed

# ----------------------------
# Export log
# ----------------------------

$logPath = Join-Path -Path $csvFolder -ChildPath $logFileName
$log | Export-Csv -LiteralPath $logPath -NoTypeInformation -Encoding UTF8

# ----------------------------
# Summary
# ----------------------------

$summaryPath = Join-Path -Path $csvFolder -ChildPath "run-summary.txt"
$summaryLines = @(
    "Summary"
    "----------------------------------------"
    "Action                           : $Action"
    "CSV file                         : $csvPath"
    "CSV delimiter used               : $effectiveDelimiter"
    "CSV column requested             : $fileNameColumn"
    "CSV column resolved              : $resolvedFileNameColumn"
    "Total rows in CSV                : $totalCsvRows"
    "File names found in CSV          : $totalCsvFileNames"
    "Unique file names in CSV         : $totalUniqueCsvFileNames"
    "Files scanned in source folder   : $totalSourceFiles"
    "CSV file names matched           : $matchedCsvNames"
    "CSV file names not found         : $notFoundCsvNames"
    "Files $($actionVerbPastTense.ToLower())                  : $processedFilesCount"
    "CSV names with multiple matches  : $multiMatchNameCount"
    "Errors during $($actionVerbPastTense.ToLower())          : $errorsCount"
    "Log file                         : $logPath"
    "----------------------------------------"
)

Write-Host ""
Write-Host $summaryLines[0] -ForegroundColor Green
$summaryLines | Select-Object -Skip 1 | ForEach-Object { Write-Host $_ }

$summaryLines | Set-Content -LiteralPath $summaryPath -Encoding UTF8
Write-Host "Summary file                     : $summaryPath"
