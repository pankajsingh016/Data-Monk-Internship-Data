import pyautogui
import time
# --- Configuration ---
# Set the interval in seconds. 120 seconds = 2 minutes.
INTERVAL_SECONDS = 10

# Set the key to press. 'shift' is a good choice as it is
# generally non-intrusive.
KEY_TO_PRESS = 'shift'

# Set the tiny distance to move the mouse.
# This value is so small it should be almost imperceptible.
MOUSE_MOVE_DISTANCE = 10000

# --- Main Loop ---
print(f"Keep-alive script started. Pressing '{KEY_TO_PRESS}' and slightly moving the mouse every {INTERVAL_SECONDS} seconds.")
print("Press Ctrl+C to stop the script.")

try:
    while True:
        # Press the specified key five times
        pyautogui.press(KEY_TO_PRESS, presses=5)
        
        # Get the current mouse position
        x, y = pyautogui.position()
        
        # Move the mouse a tiny amount to the right and back
        pyautogui.moveTo(x + MOUSE_MOVE_DISTANCE, y, duration=3)
        pyautogui.moveTo(x, y, duration=3)
        
        # Print the key press and mouse movement for logging purposes
        print(f"Pressed '{KEY_TO_PRESS}' key five times and moved mouse slightly.")
        
        # Wait for the specified interval
        time.sleep(INTERVAL_SECONDS)
        
except KeyboardInterrupt:
    print("\nKeep-alive script stopped.")
    # Exit the script gracefully when the user presses Ctrl+C
    pass
