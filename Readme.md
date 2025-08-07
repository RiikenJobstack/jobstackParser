<!-- MAacos steps-->

python3 -m venv venv
source venv/bin/activate
uvicorn app:app --reload

<!--Windows Steps -->
python -m venv venv

Open Command Prompt (cmd):

venv\Scripts\activate.bat
uvicorn app:app --reload
