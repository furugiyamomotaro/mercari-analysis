import datetime

log_file = f"log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

def log(msg):
    print(msg)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(msg + "\n")