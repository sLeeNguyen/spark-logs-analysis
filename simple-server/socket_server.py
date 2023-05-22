import random
import socket
import sys
import time

if __name__ == "__main__":
    args = sys.argv
    file_path = "../data/NASA_access_log_Aug95"
    if len(args) >= 2:
        file_path = args[1]
    log_list = open(file_path, "r")
    log_data = log_list.readline()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 9999))
    s.listen(2)
    while log_data:
        client, addr = s.accept()

        _len = len(log_data)
        if log_data[_len - 1] == "\n":
            log_data = log_data[0:_len - 1]

        print(f"data: {log_data}")
        msg = bytes(log_data, "utf8")
        client.sendall(msg)

        time_to_sleep = random.randint(1, 10)
        time.sleep(time_to_sleep)
        log_data = log_list.readline()

