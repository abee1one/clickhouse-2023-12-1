# -*- coding: utf-8 -*-
"""
ClickHouse cource. Task3: mouse handler

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
"""

# %% declaration
from datetime import datetime
from pynput import mouse, keyboard
import pygetwindow as gw

mouse_listener = None
keyboard_listener = None

def get_window_title():
    active_window = gw.getActiveWindow()
    return active_window.title if active_window else None

def on_move(x, y):
    curr_datetime = datetime.now()
    window_title = get_window_title()
    print(f'Mouse moved to ({x}, {y}) \
          datetime: {curr_datetime.strftime("%Y-%m-%d %H:%M:%S")} \
          win_title: {window_title}')

def on_click(x, y, button, pressed):
    curr_datetime = datetime.now()
    window_title = get_window_title()
    action = 'pressed' if pressed else 'released'
    print(f'Mouse {action} at ({x}, {y}) with {button} \
          datetime: {curr_datetime.strftime("%Y-%m-%d %H:%M:%S")} \
          win_title: {window_title}')

def on_scroll(x, y, dx, dy):
    curr_datetime = datetime.now()
    window_title = get_window_title()
    print(f'Scrolled at ({x}, {y}) with delta ({dx}, {dy}) \
          datetime: {curr_datetime.strftime("%Y-%m-%d %H:%M:%S")} \
          win_title: {window_title}')

def on_key_release(key):
    if key == keyboard.Key.esc:
        # Stop listeners
        mouse_listener.stop()
        keyboard_listener.stop()
        print("Listeners stopped!")
        return False    
        
# %% handlers

# Create listener instances
mouse_listener = mouse.Listener(on_move=on_move, on_click=on_click, on_scroll=on_scroll)
#keyboard_listener = keyboard.Listener(on_press=on_key_press, on_release=on_key_release)
keyboard_listener = keyboard.Listener(on_release=on_key_release)

# Start listeners
mouse_listener.start()
keyboard_listener.start()

# %% SELECT's


