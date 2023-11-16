import requests
import mysql.connector

import logging
import time
import mysql.connector

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to console
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# Also log to a file
file_handler = logging.FileHandler("cpy-errors.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) 

# Thông tin kết nối MySQL
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'btl_bigdata',
}

# # Kết nối đến MySQL
# conn = mysql.connector.connect(**mysql_config)
# print("check conn: ", conn)
# cursor = conn.cursor()

# Thông tin API key từ Polygon
api_key = 'yZXW1pS8NSbdl113puMqllhYyF4KoIbs'

# Endpoint API của Polygon
endpoint = 'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2022-01-01/2022-01-10'

# Gửi yêu cầu API
response = requests.get(endpoint, params={'apiKey': api_key})
data = response.json()

#####################################
def connect_to_mysql(config, attempts=3, delay=2):
    attempt = 1
    # Implement a reconnection routine
    while attempt < attempts + 1:
        try:
            return mysql.connector.connect(**config)
        except (mysql.connector.Error, IOError) as err:
            if (attempts is attempt):
                # Attempts to reconnect failed; returning None
                logger.info("Failed to connect, exiting without a connection: %s", err)
                return None
            logger.info(
                "Connection failed: %s. Retrying (%d/%d)...",
                err,
                attempt,
                attempts-1,
            )
            # progressive reconnect delay
            time.sleep(delay ** attempt)
            attempt += 1
    return None

cnx = connect_to_mysql(mysql_config, attempts=3)

if cnx and cnx.is_connected():

    with cnx.cursor() as cursor:

        index = 0
        for result in data['results']:
            # Thực hiện các bước xử lý dữ liệu cần thiết
            # print(str(index) + '. check result: ', result)
            index += 1
            # Ví dụ: Lưu giá đóng cửa vào bảng stock_prices
            cursor.execute("INSERT INTO stock_prices (symbol, close_price, timestamp) VALUES (%s, %s, %s)",
                           ('AAPL', result['c'], result['t']))

        result = cursor.execute("SELECT * FROM stock_prices LIMIT 20")

        rows = cursor.fetchall()

        for rows in rows:

            print(rows)
    cnx.commit()
    cursor.close()
    cnx.close()

else:

    print("Could not connect")
#####################################

#print("check data: ", data)

# Xử lý dữ liệu và lưu vào cơ sở dữ liệu MySQL
# index = 0
# for result in data['results']:
#     # Thực hiện các bước xử lý dữ liệu cần thiết
#         print(str(index) + '. check result: ', result)
#         index += 1
#     # Ví dụ: Lưu giá đóng cửa vào bảng stock_prices
#     # cursor.execute("INSERT INTO stock_prices (symbol, close_price, timestamp) VALUES (%s, %s, %s)",
#     #                ('AAPL', result['c'], result['t']))

# Commit và đóng kết nối
# conn.commit()
# cursor.close()
# conn.close()
