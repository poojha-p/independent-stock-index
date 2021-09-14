import mysql.connector
from mysql.connector import errorcode
import json
import traceback
import time
import aiohttp
import asyncio

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen


async def get_jsonparsed_data(stock_symbol):
    url = "https://financialmodelingprep.com/api/v3/quote/" + str(
        stock_symbol) + "?apikey=7151681d5db62246e86d0428532d986f"
    async with session.get(url) as response:
        results = await response.json()
        return results


async def insert_current_stock_info(json_parsed_data, cursor, myconn):
    for stock_data in json_parsed_data:
        stock_data_list = []
        for items in stock_data:
            if (items != "name" and items != "change"
                    and items != "yearHigh" and items != "yearLow"
                    and items != "priceAvg50" and items != "priceAvg200"
                    and items != "avgVolume" and items != "exchange"
                    and items != "eps" and items != "earningsAnnouncement"
                    and items != "sharesOutstanding" and items != "timestamp"):
                stock_data_list.append(stock_data[items])
        try:
            mysql_current_stock_data = "INSERT INTO current_stock_data\
(stock_symbol, current_price, change_percentage,\
day_low, day_high, market_cap, volume, open_price,\
previous_close, pe_ratio)\
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            mysql_current_stock_data_columns = [stock_data_list[0],
                                                stock_data_list[1],
                                                stock_data_list[2],
                                                stock_data_list[3],
                                                stock_data_list[4],
                                                stock_data_list[5],
                                                stock_data_list[6],
                                                stock_data_list[7],
                                                stock_data_list[8],
                                                stock_data_list[9]]
            cursor.execute(mysql_current_stock_data, mysql_current_stock_data_columns)
            myconn.commit()
        except mysql.connector.Error as err:
            print("insert failed".format(err))
            traceback.print_exc()

            delete_data = "DELETE FROM current_stock_data"
            cursor.execute(delete_data)
            print("deleted")
            myconn.commit()


async def main():
    myconn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='SchoolSucks458@',
        database='poojha_stock_index'
    )

    print("connection opened")

    cursor = myconn.cursor()

    sql_query_for_stocks = "SELECT * FROM stock_info"
    cursor.execute(sql_query_for_stocks)
    records = cursor.fetchall()

    try:
        async with aiohttp.ClientSession() as session:
            ##tasks = []
            for row in records:
                task_one = await asyncio.ensure_future(get_jsonparsed_data(row))
                task_two = asyncio.ensure_future(insert_current_stock_info(task_one, cursor, myconn))
    except mysql.connector.Error as err:
        print("another error out of insert".format(err))
        traceback.print_exc()
    finally:
        if myconn.is_connected():
            cursor.close()
            myconn.close()
            print("connection closed")


asyncio.run(main())