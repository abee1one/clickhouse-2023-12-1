# -*- coding: utf-8 -*-
"""
ClickHouse course. Task3: mouse handler
@author: abee1@yandex.ru
"""

from datetime import datetime
from pynput import mouse, keyboard
from ewmh import EWMH
import clickhouse_connect
import pandas as pd
import pandahouse as ph


def create_data_frame():
    df = pd.DataFrame(columns=["x", "y", "deltaX", "deltaY", "clientTimeStamp", "button", "target"])
    df[["x", "y", "deltaX", "deltaY"]] = df[["x", "y", "deltaX", "deltaY"]].astype(int)
    df["clientTimeStamp"] = df["clientTimeStamp"].astype(float)
    return df


def get_current_window_title():
    ewmh = EWMH()
    # Get the currently active window
    active_window = ewmh.getActiveWindow()
    if active_window:
        # Get the window title
        window_title = ewmh.getWmName(active_window)
        ewmh.display.flush()  # Ensure changes are processed
        return window_title.decode('utf-8') if window_title else None
    else:
        return None


def flush_buff(buff, buff_size):
    if buff.shape[0] >= buff_size:
        ph.to_clickhouse(buff, "mouse_movements", connection=pdh_conn, index=False)
        buff = create_data_frame()
        print(f"{buff_size} rows wrote in table")
    return buff


def on_move(x, y):
    global currX
    global currY
    global df_buff
    if currX == -1:
        currX = x
    if currY == -1:
        currY = y
    deltaX = abs(currX - x)
    deltaY = abs(currY - y)
    currX = x
    currY = y
    curr_datetime = datetime.now().timestamp()
    button_name = "None"
    window_title = get_current_window_title()
    row = [x, y, deltaX, deltaY, curr_datetime, button_name, window_title]
    df_buff.loc[len(df_buff.index)] = row
    df_buff = flush_buff(df_buff, df_buff_size)


def on_click(x, y, button, pressed):
    global currX
    global currY
    global df_buff
    if currX == -1:
        currX = x
    if currY == -1:
        currY = y
    deltaX = abs(currX - x)
    deltaY = abs(currY - y)
    currX = x
    currY = y
    curr_datetime = datetime.now().timestamp()
    button_name = button
    window_title = get_current_window_title()
    row = [x, y, deltaX, deltaY, curr_datetime, button_name, window_title]
    df_buff.loc[len(df_buff.index)] = row
    df_buff = flush_buff(df_buff, df_buff_size)


def on_scroll(x, y, dx, dy):
    global df_buff
    deltaX = abs(dx)
    deltaY = abs(dy)
    curr_datetime = datetime.now().timestamp()
    button_name = "None"
    window_title = get_current_window_title()
    row = [x, y, deltaX, deltaY, curr_datetime, button_name, window_title]
    df_buff.loc[len(df_buff.index)] = row
    df_buff = flush_buff(df_buff, df_buff_size)


def on_key_press(key):
    print("!!! KEY PRESS !!!")


def on_key_release(key):
    if key == keyboard.Key.esc:
        # Stop listeners
        mouse_listener.stop()
        keyboard_listener.stop()
        print("Listeners stopped!")
        return False


df_buff = pd.DataFrame()
df_buff_size = 50
mouse_listener = None
keyboard_listener = None
currX = -1
currY = -1

# DB connects
cli_conn = clickhouse_connect.get_client(
    host="127.0.0.1",
    port=8123,
    database="test",
    username="default",
    password="qw123456"
)
pdh_conn = {
    'host': 'http://127.0.0.1:8123',
    'database': 'test',
    'user': 'default',
    'password': 'qw123456'
}
print("CurrDB:", cli_conn.database.title())

# Drop table
query = "drop table if exists mouse_movements"
cli_conn.query(query)

# Create table
query_ddl = " \
    Create Table If Not Exists mouse_movements( \
        x Int16, \
        y Int16, \
        deltaX Int16, \
        deltaY Int16, \
        clientTimeStamp Float32, \
        button Enum( \
            'None' = 0, \
            'Button.left' = 1, \
            'Button.middle' = 2, \
            'Button.right' = 3), \
        target String \
        ) Engine = MergeTree \
          Order By clientTimeStamp"
cli_conn.query(query_ddl)


""" 
-- Create a table with Buffer engine
CREATE TABLE example_buffer_engine
(
    id UInt64,
    name String,
    value Float64
)
ENGINE = Buffer('example_remote_table', 'example_distribution_key', 16, 10000);    
"""


df_buff = create_data_frame()
# Create listener instances
mouse_listener = mouse.Listener(on_move=on_move, on_click=on_click, on_scroll=on_scroll)
keyboard_listener = keyboard.Listener(on_release=on_key_release, on_press=on_key_press)

# Start listeners
mouse_listener.start()
keyboard_listener.start()

mouse_listener.join()
keyboard_listener.join()

# Выполнить запросы:
print("количество всех движений мыши")
query = "SELECT Count(*) from mouse_movements where (deltaX + deltaY) <> 0"
df_res = cli_conn.query_df(query)
print(df_res)

print("\nкол-во движений мыши, попадающих в диапазон x < 1000 AND y < 1000 и сгруппировать по target")
query = "SELECT target, count(*) \
         FROM mouse_movements \
         WHERE x < 1000 and y < 1000 \
         GROUP BY target"
df_res = cli_conn.query_df(query)
print(df_res)

print("\nнаиболее большие движения мыши (можно посчитать с помощью дельт: plus(abs(deltaX), abs(deltaY))")
query = "SELECT target, max(plus(abs(deltaX), abs(deltaY))) as max_delta \
         FROM mouse_movements \
         GROUP BY target"
df_res = cli_conn.query_df(query)
print(df_res)

