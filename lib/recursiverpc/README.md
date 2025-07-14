$env:Path = "C:\Users\User\.local\bin;$env:Path

uv init recursiverpc --lib
uv add multiprocess dill rpyc
uv venv
uv sync
uv pip install -e .
.\.venv\Scripts\activate.ps1
uv run --script .\rpyc_classic.py
uv run .\test.py