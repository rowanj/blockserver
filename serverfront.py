import zmq
from zmq.eventloop import ioloop, zmqstream
import blockserver
import os
import zlib

class ServerFront:
    def __init__(self, blockserver=None):
        if not blockserver:
            raise ValueError('No blockserver given')
        self.blockserver = blockserver
        self.loop = None
        self.admin_stream = None

    def run(self, dir=None):
        if not dir:
            raise ValueError('No directory for server')
        self.init_admin_commands()

        print('totally running')
        ctx = zmq.Context()
        admin_skt = ctx.socket(zmq.REP)
        admin_socket_path = os.path.join(dir, 'admin')
        admin_skt.bind('ipc://' + admin_socket_path)

        self.loop = ioloop.IOLoop.instance()
        self.admin_stream = zmqstream.ZMQStream(admin_skt, self.loop)
        self.admin_stream.on_recv(self.handle_admin)

        try:
            self.loop.start()
        except KeyboardInterrupt:
            pass

    def handle_admin(self, msg):
        try:
            msg = zlib.decompress(msg[0]).decode()
            
            cmd = msg.split(' ')
            if cmd[0] in self.admin_commands:
                rep = self.admin_commands[cmd[0]](cmd[1:])
            else:
                rep = 'error: unknown command: ' + cmd[0]
        except:
            rep = 'error: invalid message'

        self.admin_stream.send(zlib.compress(rep.encode()))

    def init_admin_commands(self):
        cmds = {}
        cmds['status'] = self.admin_status
        cmds['namespace'] = self.admin_namespace
        self.admin_commands = cmds

    def admin_status(self, args):
        return 'running'

    def admin_namespace(self, args):
        return self.blockserver.get_namespace()

def main():
    script_dir = os.path.realpath(os.path.dirname(__file__))
    root_dir = os.path.join(script_dir, 'blockserver')
    try:
        os.makedirs(root_dir)
    except FileExistsError:
        pass

    config_filename = os.path.join(root_dir, 'config.json')
    config = None
    try:
        config = blockserver.load_config(config_filename)
    except:
        pass

    if not config:
        storepath = os.path.join(root_dir, 'store')
        config = blockserver.make_config(storepath=storepath, MB=1000)
    
    bs = blockserver.BlockServer(config=config)
    blockserver.save_config(filename=config_filename, config=bs.get_config())
    sf = ServerFront(blockserver=bs)

    sf.run(dir=root_dir)
    print()

if __name__ == '__main__':
    main()
