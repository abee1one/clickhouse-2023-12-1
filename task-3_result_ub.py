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

def CreateDataFrame():
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
    sButton = "None"
    window_title = get_current_window_title()
    row = [x, y, deltaX, deltaY, curr_datetime, sButton, window_title]
    df_buff.loc[len(df_buff.index)] = row
    if df_buff.shape[0] > df_buff_size:
        ph.to_clickhouse(df_buff, "mouse_movements", connection=pdh_conn, index=False)
        df_buff = CreateDataFrame()
    print(f'Mouse moved to ({x}, {y}) \
          datetime: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} \
          win_title: {window_title}')


#row = [100, 100, 10, 10, datetime.now().timestamp(), "None", "WinTitle01"]
#df.loc[len(df.index)] = row
#ph.to_clickhouse(df, "mouse_movements", connection=pdh_conn, index=False)




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


query_ddl = '''
create table test(a UInt8, b UInt8, c String) Engine MergeTree ORDER BY a;
'''

query_insert = '''
insert into test(a,b,c) VALUES (1,2,'user_1') (1, 2, 'user_3') (1, 2, 'user_5')  (1, 3, 'user_1')  (1, 3, 'user_5') 
'''

query_select = '''
select * from test where c = 'user_5'
'''




def on_click(x, y, button, pressed):
    curr_datetime = datetime.now()
    window_title = get_current_window_title()
    action = 'pressed' if pressed else 'released'
    print(f'Mouse {action} at ({x}, {y}) with {button} \
          datetime: {curr_datetime.strftime("%Y-%m-%d %H:%M:%S")} \
          win_title: {window_title}')


def on_scroll(x, y, dx, dy):
    curr_datetime = datetime.now()
    window_title = get_current_window_title()
    print(f'Scrolled at ({x}, {y}) with delta ({dx}, {dy}) \
          datetime: {curr_datetime.strftime("%Y-%m-%d %H:%M:%S")} \
          win_title: {window_title}')


def on_key_press(key):
    print("!!! KEY PRESS !!!")


def on_key_release(key):
    if key == keyboard.Key.esc:
        # Stop listeners
        mouse_listener.stop()
        keyboard_listener.stop()
        print("Listeners stopped!")
        return False


df_buff = CreateDataFrame()
# Create listener instances
mouse_listener = mouse.Listener(on_move=on_move, on_click=on_click, on_scroll=on_scroll)
keyboard_listener = keyboard.Listener(on_release=on_key_release, on_press=on_key_press)

# Start listeners
mouse_listener.start()
keyboard_listener.start()

mouse_listener.join()
keyboard_listener.join()

# Stop
# mouse_listener.stop()
# keyboard_listener.stop()


# %% SELECT's


#
