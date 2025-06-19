# dev.ps1
# Alternative development script that uses PowerShell jobs instead of separate windows

Write-Host "Starting services for development..." -ForegroundColor Green

# Start Redis and RabbitMQ
Write-Host "Starting Redis and RabbitMQ..." -ForegroundColor Cyan
docker-compose up -d redis rabbitmq

# Wait for services to be ready
Write-Host "Waiting for services to initialize..." -ForegroundColor Yellow
#Start-Sleep -Seconds 5

# Function to create a job and track its output
function Start-ServiceJob {
    param(
        [string]$Name,
        [scriptblock]$ScriptBlock
    )
    
    $job = Start-Job -Name $Name -ScriptBlock $ScriptBlock
    Write-Host "Started $Name service (JobId: $($job.Id))" -ForegroundColor Green
}

# Start FastAPI in a job
Start-ServiceJob -Name "FastAPI" -ScriptBlock {
    Set-Location -Path "$using:PWD/src"
    uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
}

Write-Host "Starting CeleryWorker..." -ForegroundColor Cyan
Start-ServiceJob -Name "CeleryWorker" -ScriptBlock {
    Set-Location -Path "$using:PWD/src"
    celery -A worker.tasks worker --loglevel=info --concurrency=1 --queues=gpu
}

# Start Celery Flower dashboard
Write-Host "Starting Celery Flower dashboard..." -ForegroundColor Cyan
Start-ServiceJob -Name "CeleryFlower" -ScriptBlock {
    Set-Location -Path "$using:PWD/src"
    celery -A worker.tasks flower --port=5555
}
Write-Host "Started Celery Flower dashboard - available at http://localhost:5555" -ForegroundColor Green

Write-Host "All services started. Use 'Get-Job' to see running jobs." -ForegroundColor Green
Write-Host "To view logs for a service, use: Receive-Job -Name <service> -Keep" -ForegroundColor Cyan
Write-Host "To stop all services, use: Get-Job | Stop-Job; docker-compose down" -ForegroundColor Cyan
Write-Host "Services available at:" -ForegroundColor Green
Write-Host "- FastAPI: http://localhost:8000" -ForegroundColor Cyan
Write-Host "- RabbitMQ: http://localhost:5672" -ForegroundColor Cyan
Write-Host "- Redis: localhost:6379" -ForegroundColor Cyan 