import os


# On an rpi, you get many addresses because it has lots of ports.
# Run this address to find which port is connected to the gateway receiver
input("Make sure device is removed, then press a key...")
b4 = os.listdir("/dev")
input("Now plug in the device, then hit any key...")
after = os.listdir("/dev")
print("Newly plugged in devices:\n")
for line in after:
    if line not in b4:
        print(line)
