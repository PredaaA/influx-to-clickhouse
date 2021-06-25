import asyncio
import os
from datetime import datetime

import asynch
from asynch.cursors import DictCursor
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import FluxRecord

print("Start", datetime.now())

load_dotenv()

influx_client = InfluxDBClient(
    os.getenv("INFLUX_ADDRESS"), os.getenv("INFLUX_TOKEN"), org=os.getenv("INFLUX_ORG")
)
influx_query_api = influx_client.query_api()

INFLUX_FIELDS_TO_CH = {
    "Bots": ("-", "bots"),
    "Humans": ("-", "humans"),
    "Discord Latency": ("-", "discord_latency"),
    "Monthly Votes": ("-", "monthlyvotes_topgg"),
    "Votes": ("-", "votes_topgg"),
    "Shards": ("-", "shard_count"),
    "Users in a VC": ("-", "users_in_vc"),
    "Users in a VC with me": ("-", "users_in_vc_with_bot"),
    "Martine Discord Member Count": ("-", "martine_discord_member_count"),
    "Martine Discord Members Connected": ("-", "martine_discord_members_connected"),
    "Martine Discord Members Online": ("-", "martine_discord_members_online"),
    "Unique Users": ("Servers", "unique_users"),
}
KEYS_TO_INSERT = [v[1] for v in INFLUX_FIELDS_TO_CH.values()]

influx_query = (
    f'from(bucket: "{os.getenv("INFLUX_BUCKET")}") '
    "|> range(start: 2018-09-01T00:00:00.000000000Z, stop: 2021-06-25T17:00:00.000000000Z) "
    # "|> range(start: 2021-06-25T16:00:00.000000000Z, stop: 2021-06-25T17:00:00.000000000Z) "
    "|> filter(fn: (r) => "
)
c = 0
for k, v in INFLUX_FIELDS_TO_CH.items():
    influx_query += f' {"or" if c > 0 else ""} (r._measurement == "{v[0]}" and r._field == "{k}")'
    c += 1
influx_query += (
    ') |> aggregateWindow(every: 5m, fn: mean, createEmpty: false) |> yield(name: "mean")'
)
print(influx_query)


start = datetime.now()
influx_resp = influx_query_api.query(influx_query)
print("Influx query done:", datetime.now() - start)

to_insert = []
for entry in influx_resp:
    for record in entry.records:
        to_insert.append(
            {
                "date": record.get_time().date(),
                "datetime": record.get_time(),
                INFLUX_FIELDS_TO_CH[record.get_field()][1]: int(record.get_value()),
            }
        )

print("Insert data done:", datetime.now() - start)


async def push_to_clickhouse():
    pool = await asynch.create_pool(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=os.getenv("CLICKHOUSE_PORT"),
        database=os.getenv("CLICKHOUSE_DATABASE"),
    )
    async with pool.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cursor:
            await cursor.execute(
                f"INSERT INTO {os.getenv('CLICKHOUSE_TABLE')}.bot({','.join(KEYS_TO_INSERT)}) VALUES",
                to_insert,
            )

    print("INSERT done:", datetime.now() - start)


if to_insert:
    asyncio.get_event_loop().run_until_complete(push_to_clickhouse())
