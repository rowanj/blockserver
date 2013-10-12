import zmq
import zlib
import sys
import os

def main(argv=None):
    server_dir = argv[1]
    if not server_dir:
        raise ValueError('No server_dir given')
    ctx = zmq.Context()
    skt = ctx.socket(zmq.REQ)
    endpoint = 'ipc://' + os.path.join(server_dir, 'admin')
    skt.connect(endpoint)
    while True:
        try:
            command = input('blockserver> ')
            req = zlib.compress(command.encode())
            skt.send(req)
            rep = skt.recv()
            response = zlib.decompress(rep).decode()
            print(response)
        except KeyboardInterrupt:
            return

def entry():
    if len(sys.argv) != 2:
        print('usage: ' + sys.argv[0] + ' server_root_directory')
        return

    main(sys.argv)
    print()

if __name__ == '__main__':
    entry()
