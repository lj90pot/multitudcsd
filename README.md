En Git Bash no uses source .venv/Scripts/activate: rompe el PATH del sistema 
y Spark falla con JAVA_GATEWAY_EXITED 
porque no encuentra cmd.exe. Invoca .venv/Scripts/python.exe directamente.