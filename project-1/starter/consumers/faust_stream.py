"""Defines trends calculations for stations"""
import faust
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: str
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: str
    station_name: str
    order: int
    line: str


def get_line(blue=False, red=False, green=False):
    if blue:
        return 'blue'
    elif red:
        return 'red'
    elif green:
        return 'green'


app = faust.App(
    "faust_stream",
    broker="kafka://localhost:9092",
    store="memory://",
    topic_partitions=1
)
topic = app.topic(
    "connector-cta-stations-stations",
    value_type=Station
)
out_topic = app.topic(
    "org.chicago.cta.station.table.v1",
    key_type=str,
    value_type=TransformedStation,
    partitions=1
)


@app.agent(topic)
async def processing(stations):
    async for station in stations:
        transformed_station = TransformedStation(
            station_id=str(station.station_id),
            station_name=station.station_name,
            order=station.order,
            line=get_line(
                blue=station.blue,
                red=station.red,
                green=station.green
            )
        )

        await out_topic.send(
            key=transformed_station.station_id,
            value=transformed_station
        )


table = app.Table(
    "transformed.station.table",
    default=list,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(out_topic)
async def transformed_station_process(out_topic):
    async for station in out_topic:
        table[station.station_id] = station


if __name__ == "__main__":
    app.main()