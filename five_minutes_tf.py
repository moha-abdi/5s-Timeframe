import json
import websockets
import asyncio
import datetime


class CandleData:
    def __init__(self, pair, timestamp, open_, close, high, low, volume, qvolume):
        self.pair = pair
        self.timestamp = timestamp
        self.close = close
        self.open = open_
        self.high = high
        self.low = low
        self.volume = volume
        self.qvolume = qvolume


async def gather_candles():
    try:
        async with websockets.connect("wss://fstream.binance.com/ws/!miniTicker@arr") as websocket:
            async for message in websocket:
                data = json.loads(message)
                pair_ = None

                for pair in data:
                    if pair['s'] == 'BTCUSDT':
                        pair_ = pair

                timestamp = int(pair_['E']) // 1000
                value = datetime.datetime.fromtimestamp(timestamp)

                yield CandleData(pair_['s'], value, pair_['o'], pair_['c'],
                                 pair_['h'], pair_['l'], pair_['v'], pair_['q'])

    except asyncio.CancelledError:
        print("Cancelled due to coroutine cancelation.")
    except Exception as e:
        print(f"An error occurred: {e}")

async def group_timestamps():
    start_time = None
    group = []

    async for candle_data in gather_candles():
        value = candle_data.timestamp

        if value.second % 5 == 0:
            start_time = value

        if start_time and (value - start_time).seconds >= 4:
            group.append(candle_data)
            yield group
            group = []
            start_time = None
        elif start_time:
            group.append(candle_data)

async def main():
    async for grouped_timestamps in group_timestamps():
        average_open = sum([float(value.open) for value in grouped_timestamps]) / len(grouped_timestamps)
        average_close = sum([float(value.close) for value in grouped_timestamps]) / len(grouped_timestamps)
        average_high = sum([float(value.high) for value in grouped_timestamps]) / len(grouped_timestamps)
        average_low = sum([float(value.low) for value in grouped_timestamps]) / len(grouped_timestamps)
        average_volume = sum([float(value.volume) for value in grouped_timestamps]) / len(grouped_timestamps)
        average_qvolume = sum([float(value.qvolume) for value in grouped_timestamps]) / len(grouped_timestamps)

        print(f"\n-- Average 5second data for {grouped_timestamps[0].pair}")
        print("Average Open:", average_open)
        print("Average Close:", average_close)
        print("Average High:", average_high)
        print("Average Low:", average_low)
        print("Average Volume:", average_volume)
        print("Average Quote Volume:", average_qvolume)
        print(f" -- Time between {grouped_timestamps[0].timestamp} - {grouped_timestamps[-1].timestamp}")


if __name__ == "__main__":
    asyncio.run(main())
