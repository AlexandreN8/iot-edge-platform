import time

def touch_heartbeat(path):
    with open(path, "w") as f:
        f.write(str(time.time()))