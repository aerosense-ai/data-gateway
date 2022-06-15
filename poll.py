# On an rPi, you get many addresses because it has lots of ports. Run this address to find which port is connected to
# the gateway receiver.
import os


input("Make sure device is removed, then press a key...")
devices_before = os.listdir("/dev")

input("Now plug in the device, then hit any key...")
devices_after = os.listdir("/dev")

print("Newly plugged in devices:\n")

for line in devices_after:
    if line not in devices_before:
        print(line)
