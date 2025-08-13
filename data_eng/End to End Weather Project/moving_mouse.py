import pyautogui
import time
import random

# --- Configuration ---
# Set the interval in seconds. 120 seconds = 2 minutes.
INTERVAL_SECONDS = 120

# Set the maximum distance to move the mouse in pixels.
# This keeps the movement subtle.
MAX_MOVEMENT = 10 

# --- Main Loop ---
print(f"Mouse jiggler started. Moving mouse every {INTERVAL_SECONDS} seconds.")
print("Press Ctrl+C to stop the script.")

try:
    while True:
        # Get the current position of the mouse
        current_x, current_y = pyautogui.position()
        
        # Generate random offsets for the mouse movement
        offset_x = random.randint(-MAX_MOVEMENT, MAX_MOVEMENT)
        offset_y = random.randint(-MAX_MOVEMENT, MAX_MOVEMENT)
        
        # Calculate the new position
        new_x = current_x + offset_x
        new_y = current_y + offset_y
        
        # Move the mouse to the new position
        # The duration=0.1 makes the move smooth and noticeable,
        # but you can set it to 0 for an instant jump.
        pyautogui.moveTo(new_x, new_y, duration=0.1)
        
        # Print the movement for logging purposes
        print(f"Moved mouse from ({current_x}, {current_y}) to ({new_x}, {new_y}).")
        
        # Wait for the specified interval
        time.sleep(INTERVAL_SECONDS)
        
except KeyboardInterrupt:
    print("\nMouse jiggler stopped.")
    # Exit the script gracefully when the user presses Ctrl+C
    pass