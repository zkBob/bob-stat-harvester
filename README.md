BOB tokans statistics crawler
====

This service on regualar basis sends requests to blockchains and CoinGecko API to collect BOB token stats on different chain and publish it in Google BigQuery.

The sources for the stats:

|      |      |
|:----:|:----:|
| total supply | BOB contract |
| collateralised circulated supply | BobVault + pools on Uniswap V3 and KyberSwap Elastic |
| trading fees | pools on Uniswap V3 and KyberSwap Elastic |
| trading bolume | BobVault and CoinGecko API |

## Configure and run the service

It assumes that the corresponding BigQuery project was created and an access key to update dataset was issued and stored in /some/path/to/biguery.key.json

1. Copy `token-deployments-info.json.example` to `token-deployments-info.json` and update `/chains/.../rpc/url` with proper endpoints.

2. Copy `docker-compose.yml.example` to `docker-compose.yml`.

3. Update the following environment variables in `docker-compose.yml`:
    - `BIGQUERY_PROJECT`
    - `BIGQUERY_DATASET`
    - `BIGQUERY_TABLE`
    - `MEASUREMENTS_INTERVAL`
    - `SNAPSHOT_DIR`
    - `TSDB_DIR`
    - `FEEDING_SERVICE_URL`
    - `FEEDING_SERVICE_PATH`
    - `FEEDING_SERVICE_HEALTH_PATH`
    - `FEEDING_SERVICE_UPLOAD_TOKEN`

4. Update the origin path to the bigquery access key in `volumes` section of `docker-compose.yml`.

5. Launch the service 

   ```bash
   docker compose up -d
   ```