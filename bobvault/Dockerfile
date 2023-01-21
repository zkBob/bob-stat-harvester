FROM python:3.9-alpine

RUN apk update && apk upgrade
# GCC.
RUN apk add --no-cache --virtual .build-deps gcc musl-dev

RUN python -m pip install --upgrade pip
COPY bobvault/requirements.txt .
RUN python -m pip install -r requirements.txt

# Remove gcc.
RUN apk del .build-deps
# Remove cache.
RUN python -m pip cache purge

WORKDIR /app

COPY abi/erc20.json abi/erc20.json
COPY abi/bobvault.json abi/bobvault.json
COPY bobvault/bobvault-trades.py .

ENTRYPOINT python bobvault-trades.py