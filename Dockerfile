FROM amancevice/pandas:1.5.0-slim

RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

RUN python -m pip cache purge

WORKDIR /app

COPY abi worker.py ./

ENTRYPOINT python worker.py