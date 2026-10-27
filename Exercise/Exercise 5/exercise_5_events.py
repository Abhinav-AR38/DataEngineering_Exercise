
import socket
import time

INPUT_FILE = "data\\iot_sample_events.txt"

HOST = "localhost"
PORT = 9999

with socket.create_connection((HOST, PORT)) as sock:
    with sock.makefile("w", encoding="utf-8") as stream:
        with open(INPUT_FILE, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                stream.write(line + "\n")
                stream.flush()

                print("Sent:", line)
                time.sleep(1)

print("Finished sending sample events.")
