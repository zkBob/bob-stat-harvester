FROM python:3.9-alpine

RUN apk update && apk upgrade
# GCC.
RUN apk add --no-cache --virtual .build-deps gcc musl-dev

RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

# Remove gcc.
RUN apk del .build-deps
# Remove cache.
RUN python -m pip cache purge

WORKDIR /app

COPY abi abi
COPY main-harvester.py .
COPY bobstats bobstats
COPY feeding feeding
COPY utils utils

ENTRYPOINT python main-harvester.py