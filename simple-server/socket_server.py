import argparse
import random
import socket
import time


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Parse arguments for log socket_server',
        epilog='End parsing arguments')
    parser.add_argument("--data_path", type=str, default='localhost',
                        help='Path to log file data')

    args = parser.parse_args()
    return args


host = 'localhost'
port = 9999

if __name__ == "__main__":
    args = parse_arguments()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)

    while True:
        print('\nListening for a client at', host, port)
        conn, addr = s.accept()
        print('\nConnected by', addr)
        try:
            print('\nReading log file...')
            with open(args.data_path) as f:
                for line in f:
                    out = line.encode('utf-8')
                    print('Sending line', line)
                    conn.send(out)
                    time.sleep(random.random() * 5)
        except socket.error:
            print('Error Occured.\n\nClient disconnected.\n')
