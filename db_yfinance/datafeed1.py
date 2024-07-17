from asyncio import get_event_loop, gather, sleep
from settrade_v2 import Investor
import pandas as pd
import numpy as np
# import pandas_ta as ta
import sqlite3
import datetime
import time
import pprint as pp

# CAF Clasic Ausiris : TFEX
investor = Investor(
    app_id        = "4TNRulJgg4Pf2mmQ",    
    app_secret    = "ANEzo2bEqc5LV4e2jrC87KCLyUtfb8DvHi+2Z/5om0QH",
    broker_id     = "063",
    app_code      = "ALGO",
    is_auto_queue = False
)

# equity   = investor.Equity(account_no="01865343")   # KGI ALGO_EQ
# equity   = investor.Equity(account_no="8301396")   # Globlex ALGO_EQ
equity   = investor.Equity(account_no="0047257")   # CAF ALGO

market   = investor.MarketData()
realtime = investor.RealtimeDataConnection()

# symbols  = ['S50U24', 'S50U24C800']
# symbols  = ['S50U24C800', 'S50U24P750']
symbols  = ['S50U24C700', 'S50U24C725', 'S50U24C750', 'S50U24C775', 'S50U24C800', 'S50U24C825', 'S50U24C850', 'S50U24C875', 'S50U24C900', 'S50U24C925', 'S50U24C950', 
            'S50U24P700', 'S50U24P725', 'S50U24P750', 'S50U24P775', 'S50U24P800', 'S50U24P825', 'S50U24P850', 'S50U24P875', 'S50U24P900', 'S50U24P925', 'S50U24P950'
            ]
res      = {}
updated_data = []

# กำหนดเวลาที่ต้องการเลื่อนการอ่านข้อมูล เป็นจำนวนวินาที
TIME_SHIFT = 0
TIMEFRAME_SECONDS = {
    '1m': 60,
    '5m': 60*5,
    '15m': 60*15,
    '1h': 60*60,
    '4h': 60*60*4,
    '1d': 60*60*24,
}
timeframe = '1m'

today  = datetime.datetime.now().strftime('%d/%m/%Y').split()[0].split('/')
open1  = datetime.datetime(int(today[2]), int(today[1]), int(today[0]), 9, 45, 00).timestamp()
close1 = datetime.datetime(int(today[2]), int(today[1]), int(today[0]), 12, 30, 00).timestamp()
open2  = datetime.datetime(int(today[2]), int(today[1]), int(today[0]), 14, 00, 00).timestamp()
close2 = datetime.datetime(int(today[2]), int(today[1]), int(today[0]), 16, 55, 00).timestamp()

weekno = datetime.datetime.today().weekday()

if weekno < 5:
    print("Weekday")
else:  # 5 Sat, 6 Sun
    print("Weekend")

class InstrumentUpdate:
    # This is a placeholder class that has a similar signature
    #   to the one from the question and should be a drop-in
    #   replacement
    def __init__(self, instrument, data, timestamp):
        self.timestamp:         int   = timestamp
        self.instrument:        str   = instrument
        self.bid:               float = data.get('bestBidPrice')
        self.offer:             float = data.get('bestOfferPrice')
        # self.high:              float = data.get('high')
        # self.low:               float = data.get('low')
        self.last:              float = data.get('last')
        self.totalVolume:       int   = data.get('totalVolume')
        self.strike:            int   = data.get('strike')
        self.intrinsicValue:    float = data.get('intrinsicValue')
        self.timeValue:         float = data.get('timeValue')
        self.openInterest:      int   = data.get('openInterest')
        self.breakEven:         float = data.get('breakEven')
        self.greekVolatility:   float = data.get('greekVolatility')
        self.theoretical:       float = data.get('theoretical')
        self.ivLast:            float = data.get('ivLast')
        self.ivBid:             float = data.get('ivBid')
        self.ivOffer:           float = data.get('ivOffer')
        self.delta:             float = data.get('delta')
        self.gamma:             float = data.get('gamma')
        self.theta:             float = data.get('theta')
        self.vega:              float = data.get('vega')
        self.lambda_:           float = data.get('lambda')
        self.moneyness:         str   = data.get('moneyness')

        # self.bid_value: float = np.random.random()
        # self.ofr_value: float = np.random.random()
        # self.time: datetime.datetime = datetime.datetime.now()
    # def get_timestamp(self) -> datetime.datetime:
    #     return self.timestamp
    def get_timestamp(self) -> int:
        return self.timestamp
    def get_instrument(self) -> str:
        return self.instrument
    def get_bid(self) -> float:
        return self.bid
    def get_offer(self) -> float:
        return self.offer
    # def get_high(self) -> float:
    #     return self.high
    # def get_low(self) -> float:
    #     return self.low
    def get_last(self) -> float:
        return self.last
    def get_totalVolume(self) -> float:
        return self.totalVolume
    def get_strike(self) -> int:
        return self.strike
    def get_intrinsicValue(self) -> float:
        return self.intrinsicValue
    def get_timeValue(self) -> float:
        return self.timeValue
    def get_openInterest(self) -> int:
        return self.openInterest
    def get_breakEven(self) -> float:
        return self.breakEven
    def get_greekVolatility(self) -> float:
        return self.greekVolatility
    def get_theoretical(self) -> float:
        return self.theoretical
    def get_ivLast(self) -> float:
        return self.ivLast
    def get_ivBid(self) -> float:
        return self.ivBid
    def get_ivOffer(self) -> float:
        return self.ivOffer
    def get_delta(self) -> float:
        return self.delta
    def get_gamma(self) -> float:
        return self.gamma
    def get_theta(self) -> float:
        return self.theta
    def get_vega(self) -> float:
        return self.vega
    def get_lambda(self) -> float:
        return self.lambda_
    def get_moneyness(self) -> str:
        return self.moneyness

    def __repr__(self):
        outstr = ''
        # outstr += (
        #     f'<InstrumentUpdate '
        #     f'timestamp={self.timestamp}>'
        #     f'instrument="{self.instrument}" '
        #     f'bid={self.bid:4.1f} '
        #     f'offer={self.offer:4.1f} '

        # )
        return outstr


class SEnergeiakosRecorder:
    # The custom data recording class

    # A name for the table in the linked database
    table_name = 'UpdateTable'

    def __init__(self, outpath: str, ):
        # Create a new sqlite database connection
        #   this also creates a new one if necessary
        self.conn = sqlite3.connect(outpath)
        # Create a cursor to that database
        self.cur  = self.conn.cursor()

        # If the table is not already in the database,
        #   add it with the correct fields
        tables = self.cur.execute('SELECT name FROM sqlite_master')

        # Python - converting a list of tuples to a list of strings
        # Ref: https://stackoverflow.com/a/11696095
        # all_tables = [ "%s %s" % x for x in tables.fetchall() ]
        all_tables = [ "%s" % x for x in tables.fetchall() ]
 
        for table in symbols:
            if not table in all_tables:
            # if not (table,) in tables.fetchall():
                self.cur.execute(
                    f'CREATE TABLE '
                    f'{table}'
                    f'(timestamp, instrument, bid, offer, last, totalVolume, strike, intrinsicValue, timeValue, openInterest, breakEven, greekVolatility, theoretical, ivLast, ivBid, ivOffer, delta, gamma, theta, vega, lambda_, moneyness, time_rec)'
                    # f'(timestamp, instrument, bid, offer, high, low, last, totalVolume, strike, intrinsicValue, timeValue, openInterest, breakEven, greekVolatility, theoretical, ivLast, ivBid, ivOffer, delta, gamma, theta, vega, lambda_, moneyness, time_rec)'
                    # f'(instrument, bid, ofr, time_req, time_rec)'
                )
        # if not (SEnergeiakosRecorder.table_name,) in tables.fetchall():
        #     self.cur.execute(
        #         f'CREATE TABLE '
        #         f'{SEnergeiakosRecorder.table_name}'
        #         f'(instrument, bid, ofr, time_req, time_rec)'
        #     )
    def append_record(self, record: InstrumentUpdate):
        # This function allows you to append one record at a time
        #   This is much slower than appending hundreds or thousands at a time
        self.cur.execute(
            f'INSERT INTO {SEnergeiakosRecorder.table_name} '
            f'VALUES(?, ?, ?, ?, ?)',
            (
                record.get_instrument(),
                record.get_bid(),
                record.get_offer(),
                record.get_time(),
                datetime.datetime.now()
            )
        )
        # Commit the change to update the database
        self.conn.commit()
    async def append_records(self, records: list[InstrumentUpdate]):
        # This function allows you to append many records at a time
        #   This can be much faster than iterating over each one
        print(records)
        for record in records:
            # print("intert into table name >>>", record.instrument)
            self.cur.execute(
            f'INSERT INTO {record.instrument} '
            f'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (
                record.get_timestamp(),
                record.get_instrument(),
                record.get_bid(),
                record.get_offer(),
                # record.get_high(),
                # record.get_low(),
                record.get_last(),
                record.get_totalVolume(),
                record.get_strike(),
                record.get_intrinsicValue(),
                record.get_timeValue(),
                record.get_openInterest(),
                record.get_breakEven(),
                record.get_greekVolatility(),
                record.get_theoretical(),
                record.get_ivLast(),
                record.get_ivBid(),
                record.get_ivOffer(),
                record.get_delta(),
                record.get_gamma(),
                record.get_theta(),
                record.get_vega(),
                record.get_lambda(),
                record.get_moneyness(),
                datetime.datetime.now(),
            )
        )
        # self.cur.executemany(
        #     f'INSERT INTO {SEnergeiakosRecorder.table_name} '
        #     f'VALUES(?, ?, ?, ?, ?)',
        #     [
        #         (
        #             record.get_instrument(),
        #             record.get_bid_value(),
        #             record.get_ofr_value(),
        #             record.get_time(),
        #             datetime.datetime.now()
        #         ) for record in records
        #     ]
        # )
        # Commit the change to update the database
        self.conn.commit()
    def get_records(self):
        return self.cur.execute(
            f'SELECT * FROM {SEnergeiakosRecorder.table_name}'
        ).fetchall()


async def get_instrument_update(instrument, data, timestamp):
    # A dummy function that returns an object matching the one
    #   shown in the example
    # This function would be replaced with whatever you real data
    #   source is
    # update = InstrumentUpdate(instrument, data, timestamp)
    updated_data.append(InstrumentUpdate(instrument, data, timestamp))
    # print(updated_data)
    return updated_data

def _gt(s: float = 0.0) -> float:
    # Convenience function for getting time elapsed
    return time.perf_counter() - s

def main_test(per_loop_sleep: float = None, total_upates: int = int(1e4)):
    # The fname of the sqlite database
    dbname = 'test.sqlite3'
    # Some various names to iterate over to simulate having a variety of data
    instruments = ['abc', 'cde', 'efg', 'ghi', 'ijk']
    # Link into the database (or create it if it doesn't exist)
    ser = SEnergeiakosRecorder(dbname)
    # Record how many ticks we get
    ticks = 0
    # Record the start time for profiling speed
    start_time = _gt()
    # Create a list that updates to the database will be temporarily stored in
    updates = []
    # Wrap in a try-except block so that we can
    #   ctrl-c out if necessary without breaking anything
    try:
        while True:
            # Sleep to similar lower update rates
            if per_loop_sleep:
                time.sleep(per_loop_sleep)
            # Get an update for each instrument using the dummy function
            for instrument in instruments:
                update = get_instrument_update(instrument)
                # Add the collected updates to the database buffer
                updates.append(update)
            # By only appending every so often, we greatly decrease
            #   the database IO overhead
            if len(updates) >= 5000:
                ser.append_records(updates)
                updates = []
            ticks += len(instruments)
            # If we got enough, stop
            if ticks >= total_upates:
                break

    except KeyboardInterrupt:
        print('exited prematurely')
        pass
    # If there is anything left in the buffer, add it
    if updates:
        ser.append_records(updates)
    # Print run stats
    print(f'Total updates: {total_upates: 7} - Delay: {per_loop_sleep or 0.0:6.4f}')
    print(f'Run time: {_gt(start_time):.1f} sec')
    print(f'Run rate: {ticks / _gt(start_time):.0f} records / sec')
    print()


def get_data_test():
    # Link into the same database
    ser = SEnergeiakosRecorder('test.sqlite3')
    # print it somewhat formatted to see what's in the table
    print('  ' + '\n  '.join([str(x) for x in ser.get_records()]))
    print()


async def fetch_price(symbol, timestamp):
    # print("symbol = ", symbol, ">>>", timestamp)

    try:
        # res = await market.get_quote_symbol(symbol)
        res[symbol] = market.get_quote_symbol(symbol)
        # timeframe_secs = TIMEFRAME_SECONDS[timeframe]
        # print("timeframe_secs = ", timeframe_secs)

        return res

    except Exception as e:
        print('----->', symbol)
        print(type(e).__name__, str(e))


async def main():
    t1  = time.time()
    # The fname of the sqlite database
    dbname = 'test.sqlite3'
    # Link into the database (or create it if it doesn't exist)
    ser = SEnergeiakosRecorder(dbname)
    t2  = (time.time()) - t1
    print(f'ใช้เวลาเชื่อมต่อ SQLite : {t2:0.2f} วินาที')
    # print(f'ใช้เวลาหาว่ามีเหรียญ เทรดfuture : {t2:0.2f} วินาที')

    time_wait = TIMEFRAME_SECONDS[timeframe] # กำหนดเวลาต่อ 1 รอบ

    #==================================================================
    # อ่านแท่งเทียนย้อนหลัง
    t1 = time.time()
    local_time = time.ctime(t1)
    print(f'เริ่มอ่านแท่งเทียนย้อนหลัง ที่ {local_time}')

    # อ่านแท่งเทียนแบบ async และ เทรดตามสัญญาน
    # loops = [fetch_ohlcv_trade(exchange, symbol, timeframe, limit, **kwargs) for symbol in symbols]
    # await gather(*loops)

    t2 = (time.time()) - t1
    print(f'อ่านแท่งเทียนทุกเหรียญใช้เวลา : {t2:0.2f} วินาที')

    #==================================================================
    count = 0
    status = ('|','/','-','\\')

    next_ticker  = time.time()
    next_ticker -= (next_ticker % time_wait)        # ตั้งรอบเวลา
    next_ticker += time_wait                        # กำหนดรอบเวลาถัดไป

    open3  = datetime.datetime(int(today[2]), int(today[1]), int(today[0]), 17, 5, 00).timestamp()  # Testing Purpose
    close3 = datetime.datetime(int(today[2]), int(today[1]), int(today[0]), 17, 5, 30).timestamp()  # Testing Purpose

    try:
        while True:
            seconds = time.time()
            # print("next_ticker = ", next_ticker)
            # print("second = ", seconds)
            # if (seconds >= next_ticker + TIME_SHIFT) and (((seconds >= open1) and (seconds <= close1)) or ((seconds >= open2) and (seconds <= close2))):     # ครบรอบ
            # if (seconds >= next_ticker + TIME_SHIFT) and (((next_ticker >= open1) and (next_ticker <= close1)) or ((next_ticker >= open2) and (next_ticker <= close2))):     # ครบรอบ
            if (seconds >= next_ticker + TIME_SHIFT):     # ครบรอบ
                # if (((seconds >= open1) and (seconds <= close1+2)) or ((seconds >= open2) and (seconds <= close2+2))):
                if (((seconds >= open3) and (seconds <= close3+2)) or ((seconds >= open2) and (seconds <= close2))):    # Testing Purpose
                    print("seconds = ", seconds)
                    local_time = time.ctime(seconds)
                    print(f'เริ่มเช็คค่าอินดิเคเตอร์ ที่ {local_time}')
                    #==================================================================
                    t1 = time.time()
                    # อ่านแท่งเทียนแบบ async และ เทรดตามสัญญาน
                    # loops = [fetch_ohlcv_trade(exchange, symbol, timeframe, limit, next_ticker, **kwargs) for symbol in symbols]
                    # await gather(*loops)
                    loops = [fetch_price(symbol, next_ticker) for symbol in symbols]
                    await gather(*loops)
                    # pp.pprint(res)
                    # print(res['S50U24C800'].get('bestBidPrice'))
                    # pp.pprint(res['S50U24C800'])
                    loops = [get_instrument_update(symbol, res[symbol], next_ticker) for symbol in symbols]
                    await gather(*loops)                    

                    global updated_data
                    await ser.append_records(updated_data)
                    updated_data = []

                    # next_ticker += time_wait                # กำหนดรอบเวลาถัดไป
                    t2 = (time.time()) - t1
                    print(f'ตรวจสอบอินดิเคเตอร์ทุกเหรียญใช้เวลา : {t2:0.2f} วินาที')

                next_ticker += time_wait                # กำหนดรอบเวลาถัดไป
            
            await sleep(1)
            print(status[count%4], end='\r')
            count += 1
            count = count%4

    except KeyboardInterrupt:
        print('exited prematurely')
        pass
    # If there is anything left in the buffer, add it
    if updated_data:
        ser.append_records(updated_data)
    # Print run stats
    # print(f'Total updates: {total_upates: 7} - Delay: {per_loop_sleep or 0.0:6.4f}')
    # print(f'Run time: {_gt(start_time):.1f} sec')
    # print(f'Run rate: {ticks / _gt(start_time):.0f} records / sec')
    # print()

if __name__ == "__main__":
    # Verify we can add data
    # main_test(total_upates=1)
    # Verify we can get the data back
    # get_data_test()
    loop = get_event_loop()
    loop.run_until_complete(main())

