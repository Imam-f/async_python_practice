$env:Path = "C:\Users\User\.local\bin;$env:Path

uv init recursiverpc --lib
uv add multiprocess dill rpyc
uv venv
uv sync
uv pip install -e .
.\.venv\Scripts\activate.ps1
uv run --script .\rpyc_classic.py
uv run .\test.py
New-ItemProperty -Path "HKLM:\SOFTWARE\OpenSSH" -Name DefaultShell -Value "C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe" -PropertyType String -Force
New-ItemProperty -Path "HKLM:\SOFTWARE\OpenSSH" -Name DefaultShell -Value "C:\Program Files\Git\bin\bash.exe" -PropertyType String -Force

dependency
    uv
    rpyc
    paramiko
    plumbum
    ssh
    dill
