# Iniciar la auditoría: Todo lo que pase se guardará en este log
$fecha = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = ".\auditoria_semma_$fecha.log"
Start-Transcript -Path $logFile

Write-Host "======================================================" -ForegroundColor Cyan
Write-Host " INICIANDO PIPELINE DE DATOS - ELECTROPUNO (SEMMA)" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan

# Directorio de logs por fase/modelo
$logsDir = ".\logs"
if (-Not (Test-Path $logsDir)) { New-Item -ItemType Directory -Path $logsDir | Out-Null }

# Helper: ejecutar comando y guardar salida (stdout+stderr) en log
function Run-Log {
	param(
		[string]$cmd,
		[string]$logPath
	)
	Write-Host "Ejecutando: $cmd" -ForegroundColor DarkCyan
	try {
		$output = Invoke-Expression $cmd 2>&1
		$output | Tee-Object -FilePath $logPath
	} catch {
		$_ | Out-File -FilePath $logPath -Append
	}
}

# 1. Levantando infraestructura
Write-Host "`n[1/5] Levantando contenedores Docker..." -ForegroundColor Yellow
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "01_levantar_infra_$stepTime.log"
Run-Log "docker compose -f .\docker-compose.yml up -d --force-recreate namenode datanode resourcemanager nodemanager spark-master spark-worker" $stepLog

# IMPORTANTE: Esperar a que Hadoop salga del "Safe Mode" antes de meterle datos
Write-Host "Esperando 15 segundos para que HDFS inicie correctamente..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# 2. Preparando HDFS e Ingestando Datos
Write-Host "`n[2/5] Creando directorios y subiendo data.csv a HDFS..." -ForegroundColor Yellow
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "02_ingest_$stepTime.log"
Run-Log "docker exec namenode hdfs dfs -mkdir -p /user/hadoop/electropuno" $stepLog
Run-Log "docker cp .\data.csv namenode:/tmp/electropuno.csv" $stepLog
Run-Log "docker exec namenode hdfs dfs -put -f /tmp/electropuno.csv /user/hadoop/electropuno/" $stepLog

# 3. Fase Sample y Explore
Write-Host "`n[3/5] Ejecutando Fases S y E (Sample & Explore)..." -ForegroundColor Yellow
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "03_sample_explore_$stepTime.log"
Run-Log "docker cp .\data.csv spark-master:/opt/work/data.csv" $stepLog
Run-Log "docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/sample/inspect_sample.py --input /opt/work/data.csv" $stepLog
Run-Log "docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/explore/explore_puno.py --input hdfs://namenode:9000/user/hadoop/electropuno/electropuno.csv" $stepLog

# 4. Fase Modify
Write-Host "`n[4/5] Ejecutando Fase M (Modify) - Limpiando datos..." -ForegroundColor Yellow
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "04_modify_$stepTime.log"
Run-Log "docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/modify/modify_puno.py" $stepLog

# 5. Fase Model y Assess
Write-Host "`n[5/5] Ejecutando Fases M y A (Model & Assess) - Entrenando IA..." -ForegroundColor Yellow

# Build dataset
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "05_build_model_dataset_$stepTime.log"
Run-Log "docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/build_model_dataset_puno.py" $stepLog

# Regression model
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "05_model_regression_$stepTime.log"
Run-Log "docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/model_regression_puno.py" $stepLog

# Actualizando el script de K-Means corregido antes de ejecutarlo
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "05_model_kmeans_copy_$stepTime.log"
Run-Log "docker cp .\scripts\model\model_kmeans_puno.py spark-master:/opt/work/scripts/model/model_kmeans_puno.py" $stepLog

$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "05_model_kmeans_$stepTime.log"
Run-Log "docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/model_kmeans_puno.py" $stepLog

# Random Forest
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "05_model_random_forest_$stepTime.log"
Run-Log "docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/model_random_forest_puno.py" $stepLog

# Assess model
$stepTime = Get-Date -Format "yyyyMMdd_HHmmss"
$stepLog = Join-Path $logsDir "05_assess_model_$stepTime.log"
Run-Log "docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/assess_model_puno.py" $stepLog

Write-Host "`n======================================================" -ForegroundColor Cyan
Write-Host " PIPELINE FINALIZADO CON ÉXITO" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "Logs por fase y modelo guardados en: $logsDir" -ForegroundColor Green

# Cerrar la auditoría
Stop-Transcript
Write-Host "Log de auditoría guardado en: $logFile" -ForegroundColor Green