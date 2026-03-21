param(
    [string]$Profile = "shekartelstra",
    [string]$WorkspaceRoot = "/Workspace/Users/shekartelstra@gmail.com/RetailPulse"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$stageRoot = Join-Path $env:TEMP "retailpulse_databricks_sync"

$pathsToSync = @(
    ".ai",
    ".vscode",
    "config",
    "src"
)

if (Test-Path $stageRoot) {
    Remove-Item -Recurse -Force $stageRoot
}

New-Item -ItemType Directory -Path $stageRoot | Out-Null

foreach ($path in $pathsToSync) {
    $sourcePath = Join-Path $repoRoot $path
    $stagedPath = Join-Path $stageRoot $path

    New-Item -ItemType Directory -Path $stagedPath -Force | Out-Null

    robocopy $sourcePath $stagedPath /E /XD ".databricks" | Out-Null

    if ($LASTEXITCODE -ge 8) {
        throw "robocopy failed for $path with exit code $LASTEXITCODE"
    }

    databricks workspace import-dir $stagedPath "$WorkspaceRoot/$path" --overwrite --profile $Profile
}

$notebooksSource = Join-Path $repoRoot "notebooks"
$notebooksStage = Join-Path $stageRoot "notebooks"
New-Item -ItemType Directory -Path $notebooksStage -Force | Out-Null
robocopy $notebooksSource $notebooksStage /E /XD ".databricks" | Out-Null

if ($LASTEXITCODE -ge 8) {
    throw "robocopy failed for notebooks with exit code $LASTEXITCODE"
}

Get-ChildItem -Path $notebooksStage -Recurse -File | ForEach-Object {
    $relativePath = $_.FullName.Substring($notebooksStage.Length + 1).Replace("\", "/")
    $targetPath = "$WorkspaceRoot/notebooks/$relativePath"

    if ($_.Extension -eq ".py") {
        $targetNotebookPath = $targetPath.Substring(0, $targetPath.Length - 3)
        databricks workspace import $targetNotebookPath --file $_.FullName --format SOURCE --language PYTHON --overwrite --profile $Profile
    }
    else {
        databricks workspace import $targetPath --file $_.FullName --format AUTO --overwrite --profile $Profile
    }
}

$readmePath = Join-Path $repoRoot "README.MD"
databricks workspace import "$WorkspaceRoot/README.MD" --file $readmePath --format AUTO --overwrite --profile $Profile

Remove-Item -Recurse -Force $stageRoot

Write-Host "RetailPulse synced to $WorkspaceRoot using profile $Profile"
