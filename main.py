import asyncio
import os
from collections import defaultdict
from datetime import datetime

from aiochclient.client import ChClient
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

print("Start", datetime.now())

load_dotenv()

influx_client = InfluxDBClient(
    os.getenv("INFLUX_ADDRESS"), os.getenv("INFLUX_TOKEN"), org=os.getenv("INFLUX_ORG")
)
influx_query_api = influx_client.query_api()

SUB = "bot"
INFLUX_FIELDS_TO_CH = {
    "Bots": "bots",
    "Humans": "humans",
    "Discord Latency": "discord_latency",
    "Monthly Votes": "monthlyvotes_topgg",
    "Votes": "votes_topgg",
    "Shards": "shard_count",
    "Users in a VC": "users_in_vc",
    "Users in a VC with me": "users_in_vc_with_bot",
    "Martine Discord Member Count": "martine_discord_member_count",
    "Martine Discord Members Connected": "martine_discord_members_connected",
    "Martine Discord Members Online": "martine_discord_members_online",
    "Unique Users": "unique_users",
}
SUB = "guilds"
INFLUX_FIELDS_TO_CH = {
    "Total": "total",
    "Unavailable": "unavailable",
    "Total Users": "total_users",
    "Large": "large",
    "Unchunked": "unchunked",
    "Server Channels": "total_channels",
    "Text Channels": "text_channels",
    "Voice Channels": "voice_channels",
}
SUB = "audio"
INFLUX_FIELDS_TO_CH = {
    "Active Music Players": "music_players_active",
    "Inactive Music Players": "music_players_inactive",
    "Music Players": "music_players_total",
}
SUB = "currency"
INFLUX_FIELDS_TO_CH = {"Currency In Circulation": "marts"}

influx_query = (
    f'from(bucket: "{os.getenv("INFLUX_BUCKET")}") '
    "|> range(start: 2018-09-01T00:00:00.000000000Z, stop: now()) "
    # "|> range(start: 2021-06-25T16:00:00.000000000Z, stop: 2021-06-25T17:00:00.000000000Z) "
    "|> filter(fn: (r) => "
)
c = 0
for k, v in INFLUX_FIELDS_TO_CH.items():
    influx_query += f' {"or" if c > 0 else ""} (r._field == "{k}")'
    c += 1
influx_query += (
    ') |> aggregateWindow(every: 5m, fn: mean, createEmpty: false) |> yield(name: "mean")'
)
print(influx_query)


start = datetime.now()
influx_resp = influx_query_api.query(influx_query)
print("Influx query done:", datetime.now() - start)


def default_dict():
    return {k: None for k in INFLUX_FIELDS_TO_CH.values()}


to_insert = defaultdict(default_dict)
keys_to_insert = ["date", "datetime"]
for entry in influx_resp:
    for record in entry.records:
        key = INFLUX_FIELDS_TO_CH[record.get_field()]
        if key not in keys_to_insert:
            keys_to_insert.append(key)
        record_datetime = record.get_time().replace(tzinfo=None)
        to_insert[record_datetime].update(
            {
                "date": record_datetime.date(),
                "datetime": record_datetime,
                key: int(record.get_value()),
            }
        )

print("Insert data created:", datetime.now() - start)


async def push_to_clickhouse():
    ch_client = ChClient(
        url=os.getenv("CLICKHOUSE_URL"),
        database=os.getenv("CLICKHOUSE_DATABASE"),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PWD"),
    )
    await ch_client.execute(
        f"INSERT INTO {os.getenv('CLICKHOUSE_TABLE')}.{SUB}({','.join(keys_to_insert)}) VALUES",
        list(to_insert.values()),
    )
    # await cursor.execute(
    #     f"ALTER TABLE {os.getenv('CLICKHOUSE_TABLE')}.{SUB} DELETE WHERE toYYYYMMDD(datetime) BETWEEN 20210624 AND 20210626"
    # )
    # await cursor.execute(
    #     f"ALTER TABLE {os.getenv('CLICKHOUSE_TABLE')}.{SUB} DELETE WHERE unique_users=0"
    # )
    await ch_client.close()

    print("INSERT done:", datetime.now() - start)


if to_insert:
    asyncio.get_event_loop().run_until_complete(push_to_clickhouse())
