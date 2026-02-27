
# ----------------------------
# Configuration
# ----------------------------

$csvFolder = "\\ysvvstore11\FS_YSL_YFB4-tijdelijk\Albert\Infectie preventie"
$sourceFolder = "\\ysvvstore11\FS_YSL_YFB4-nietmedisch\FS_YSL_YFB4-nietmedisch\Hans\Personeelsdossier"
$destinationFolder = "\\ysvvstore11\FS_YSL_YFB4-medisch\FS_YSL_YFB4-medisch\Infectie Preventie\Medewerkers"
$csvFileName        = "Infectie Preventie - Medewerker - VDMW - vaccinatie documenten.csv"

# Set the CSV delimiter.
# In Dutch CSV files this is often ';'
$csvDelimiter       = ';'

# Set the CSV column that contains the file names.
# Leave empty ('') to automatically use the first column.
$fileNameColumn     = 'file'

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

$rows = Import-Csv -LiteralPath $csvPath -Delimiter $csvDelimiter

if (-not $rows -or $rows.Count -eq 0) {
    throw "The CSV file is empty."
}

# Automatically use the first column if no column name is specified
if ([string]::IsNullOrWhiteSpace($fileNameColumn)) {
    $fileNameColumn = $rows[0].PSObject.Properties.Name[0]
}

if ($fileNameColumn -notin $rows[0].PSObject.Properties.Name) {
    throw "Column '$fileNameColumn' was not found in the CSV."
}

# Extract file names from the CSV
$csvFileNames = $rows |
    ForEach-Object { [string]($_.$fileNameColumn).Trim() } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

$totalCsvRows            = $rows.Count
$totalCsvFileNames       = $csvFileNames.Count
$uniqueCsvFileNames      = $csvFileNames | Sort-Object -Unique
$totalUniqueCsvFileNames = $uniqueCsvFileNames.Count

# ----------------------------
# Read source folder files
# ----------------------------

Write-Host ""
Write-Host "Reading source folder files..." -ForegroundColor Cyan

# This loads all files first so we know the total count and can show accurate progress
$sourceFiles = Get-ChildItem -LiteralPath $sourceFolder -Recurse -File
$totalSourceFiles = $sourceFiles.Count

# Build an index by file name for fast lookup
$fileIndex = @{}
$indexStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

for ($i = 0; $i -lt $totalSourceFiles; $i++) {
    $file = $sourceFiles[$i]

    if (-not $fileIndex.ContainsKey($file.Name)) {
        $fileIndex[$file.Name] = New-Object System.Collections.ArrayList
    }

    [void]$fileIndex[$file.Name].Add($file)

    $processed = $i + 1
    $percent   = [int](($processed / [Math]::Max($totalSourceFiles, 1)) * 100)

    $elapsedSeconds = [Math]::Max($indexStopwatch.Elapsed.TotalSeconds, 1)
    $ratePerSecond  = $processed / $elapsedSeconds
    $remainingItems = $totalSourceFiles - $processed
    $secondsLeft    = if ($ratePerSecond -gt 0) { [int]($remainingItems / $ratePerSecond) } else { -1 }

    Write-Progress `
        -Id 1 `
        -Activity "Indexing source files" `
        -Status "$processed of $totalSourceFiles" `
        -PercentComplete $percent `
        -SecondsRemaining $secondsLeft
}

Write-Progress -Id 1 -Activity "Indexing source files" -Completed

# ----------------------------
# Helper function
# ----------------------------

function Get-UniqueDestinationPath {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Folder,

        [Parameter(Mandatory = $true)]
        [string]$FileName
    )

    $baseName  = [System.IO.Path]::GetFileNameWithoutExtension($FileName)
    $extension = [System.IO.Path]::GetExtension($FileName)
    $candidate = Join-Path -Path $Folder -ChildPath $FileName
    $counter   = 1

    while (Test-Path -LiteralPath $candidate) {
        $newName   = "{0} ({1}){2}" -f $baseName, $counter, $extension
        $candidate = Join-Path -Path $Folder -ChildPath $newName
        $counter++
    }

    return $candidate
}

# ----------------------------
# Process CSV file names
# ----------------------------

Write-Host ""
Write-Host "Processing CSV file names..." -ForegroundColor Cyan

$matchedCsvNames      = 0
$notFoundCsvNames     = 0
$movedFilesCount      = 0
$multiMatchNameCount  = 0
$errorsCount          = 0

$log = New-Object System.Collections.Generic.List[object]
$processStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

for ($i = 0; $i -lt $totalUniqueCsvFileNames; $i++) {
    $fileName = $uniqueCsvFileNames[$i]

    $processed = $i + 1
    $percent   = [int](($processed / [Math]::Max($totalUniqueCsvFileNames, 1)) * 100)

    $elapsedSeconds = [Math]::Max($processStopwatch.Elapsed.TotalSeconds, 1)
    $ratePerSecond  = $processed / $elapsedSeconds
    $remainingItems = $totalUniqueCsvFileNames - $processed
    $secondsLeft    = if ($ratePerSecond -gt 0) { [int]($remainingItems / $ratePerSecond) } else { -1 }

    Write-Progress `
        -Id 2 `
        -Activity "Looking up and moving files" `
        -Status "$processed of $totalUniqueCsvFileNames : $fileName" `
        -PercentComplete $percent `
        -SecondsRemaining $secondsLeft

    if ($fileIndex.ContainsKey($fileName)) {
        $matches = $fileIndex[$fileName]
        $matchedCsvNames++

        if ($matches.Count -gt 1) {
            $multiMatchNameCount++
        }

        foreach ($match in $matches) {
            try {
                $destinationPath = Get-UniqueDestinationPath -Folder $destinationFolder -FileName $match.Name

                Move-Item -LiteralPath $match.FullName -Destination $destinationPath -Force

                $movedFilesCount++

                $log.Add([PSCustomObject]@{
                    FileName         = $fileName
                    Status           = "Moved"
                    SourcePath       = $match.FullName
                    DestinationPath  = $destinationPath
                    Message          = "File moved successfully"
                })
            }
            catch {
                $errorsCount++

                $log.Add([PSCustomObject]@{
                    FileName         = $fileName
                    Status           = "Error"
                    SourcePath       = $match.FullName
                    DestinationPath  = $null
                    Message          = $_.Exception.Message
                })
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
    }
}

Write-Progress -Id 2 -Activity "Looking up and moving files" -Completed

# ----------------------------
# Export log
# ----------------------------

$logPath = Join-Path -Path $csvFolder -ChildPath "move-log.csv"
$log | Export-Csv -LiteralPath $logPath -NoTypeInformation -Encoding UTF8

# ----------------------------
# Summary
# ----------------------------

Write-Host ""
Write-Host "Summary" -ForegroundColor Green
Write-Host "----------------------------------------"
Write-Host "CSV file                         : $csvPath"
Write-Host "CSV column used                  : $fileNameColumn"
Write-Host "Total rows in CSV                : $totalCsvRows"
Write-Host "File names found in CSV          : $totalCsvFileNames"
Write-Host "Unique file names in CSV         : $totalUniqueCsvFileNames"
Write-Host "Files found in source folder     : $totalSourceFiles"
Write-Host "CSV file names matched           : $matchedCsvNames"
Write-Host "CSV file names not found         : $notFoundCsvNames"
Write-Host "Files moved                      : $movedFilesCount"
Write-Host "CSV names with multiple matches  : $multiMatchNameCount"
Write-Host "Errors during move               : $errorsCount"
Write-Host "Log file                         : $logPath"
Write-Host "----------------------------------------"