# Activate virtual environment
& "D:/fastcite/fastcite_backend/.venv/Scripts/Activate.ps1"

# Ensure logs folder exists
if (!(Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" | Out-Null
}

Write-Host "Starting Celery workers (live output)...`n"

# Start each Celery worker in a new PowerShell process that stays visible inside VS Code terminal
Start-Process powershell -ArgumentList '-NoExit', '-Command', "& { celery -A celery_app.celery_app.celery_app worker -Q chatbot -n chatbot_worker@host --loglevel=info --pool=gevent --concurrency=50}"
Start-Process powershell -ArgumentList '-NoExit', '-Command', "& { celery -A celery_app.celery_app.celery_app worker -Q uploads -n upload_worker@host --loglevel=info --pool=gevent --concurrency=4}"
Start-Process powershell -ArgumentList '-NoExit', '-Command', "& { celery -A celery_app.celery_app.celery_app worker -Q maintenance -n maintenance_worker@host --loglevel=info --pool=gevent --concurrency=2}"
Start-Process powershell -ArgumentList '-NoExit', '-Command', "& { celery -A celery_app.celery_app.celery_app worker -Q default -n default_worker@host --loglevel=info --pool=gevent --concurrency=10}"

Write-Host "`nAll Celery workers started!"
Write-Host "---------------------------------------------"
Write-Host "Monitor logs inside each opened terminal tab."
Write-Host "---------------------------------------------"
