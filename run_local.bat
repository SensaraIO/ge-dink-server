@echo off
REM Start locally on http://127.0.0.1:8000
python -m venv venv
call venv\Scripts\activate
pip install -r requirements.txt
uvicorn server:app --host 127.0.0.1 --port 8000 --reload
