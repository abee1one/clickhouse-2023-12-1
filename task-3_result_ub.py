# -*- coding: utf-8 -*-
"""
ClickHouse course. Task3: mouse handler
@author: abee1@yandex.ru
"""

# %% create table SQL
"""
Create Table mouse_movements(
    x Int16,
    y Int16,
    deltaX Int16,
    deltaY Int16,
    clientTimeStamp Float32,
    button Enum(
        "left" = -1,
        "middle" = 0,
        "right" = 1),
    target String
    )
    
-- Create a table with Buffer engine
CREATE TABLE example_buffer_engine
(
    id UInt64,
    name String,
    value Float64
)
ENGINE = Buffer('example_remote_table', 'example_distribution_key', 16, 10000);    
    
"""


from datetime import datetime
from pynput import mouse, keyboard
from ewmh import EWMH
# from infi.clickhouse_orm import Database

mouse_listener = None
keyboard_listener = None

HOST = 'http://127.0.0.1:8123'

query_ddl = '''
create table test(a UInt8, b UInt8, c String) Engine MergeTree ORDER BY a;
'''

query_insert = '''
insert into test(a,b,c) VALUES (1,2,'user_1') (1, 2, 'user_3') (1, 2, 'user_5')  (1, 3, 'user_1')  (1, 3, 'user_5') 
'''

query_select = '''
select * from test where c = 'user_5'
'''


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
    curr_datetime = datetime.now()
    window_title = get_current_window_title()
    print(f'Mouse moved to ({x}, {y}) \
          datetime: {curr_datetime.strftime("%Y-%m-%d %H:%M:%S")} \
          win_title: {window_title}')


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
