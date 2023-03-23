BOB tokens statistics harvester
====

This service on regualar basis sends requests to blockchains and CoinGecko API to collect BOB token stats on different chain, keeps it in a local timeseries database and publishes the stats on [the BOB token stats server](https://github.com/zkBob/bob-circulating-supply).

The sources for the stats:

|      |      |
|:----:|:----:|
| total supply | BOB contract |
| collateralised circulated supply | BobVault + pools on Uniswap V3 and KyberSwap Elastic |
| trading fees | pools on Uniswap V3 and KyberSwap Elastic |
| trading bolume | BobVault and CoinGecko API |
| balances | BOB contract |

## Configure and run the service

It assumes that the corresponding BigQuery project was created and an access key to update dataset was issued and stored in /some/path/to/biguery.key.json

1. Copy `token-deployments-info.json.example` to `token-deployments-info.json` and update `/chains/.../rpc/url` and `/chains/.../inventories/[protocol:BobVault]/feeding_service_path` with proper endpoints.

2. Copy `docker-compose.yml.example` to `docker-compose.yml`.

3. Update the following environment variables in `docker-compose.yml`:
    - `MEASUREMENTS_INTERVAL`
    - `SNAPSHOT_DIR`
    - `TSDB_DIR`
    - `FEEDING_SERVICE_URL`
    - `FEEDING_SERVICE_PATH`
    - `FEEDING_SERVICE_UPLOAD_TOKEN`

4. Launch the service 

   ```bash
   docker compose up -d
   ```