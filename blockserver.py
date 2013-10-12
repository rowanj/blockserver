import uuid
import json
import os

from tornado.ioloop import IOLoop
from zmq.eventloop.ioloop import ZMQPoller

class BlockServer:
    def __init__(self, config=None):
        if not config:
            raise ValueError("no configuration given")

        self.config = config
        if 'peers' not in self.config:
            self.config['peers'] = {}

    def get_config(self):
        return self.config

    def get_namespace(self):
        return self.config['namespace']

    def get_id(self):
        return self.config['id']

    def get_share_peers(self):
        result = {}
        peers_dict = self.config['peers']
        for peer_id in peers_dict:
            peerinfo = {'addresses':peers_dict[peer_id]['addresses']}
            result[peer_id] = peerinfo
        return result

    def handle_join_request(self, candidate):
        new_peer_id = candidate['id']
        if new_peer_id in self.config['peers']:
            raise RuntimeError('peer already added')

        new_peer_entry = {'addresses':[candidate['source_address']]}
        self.config['peers'][new_peer_id] = new_peer_entry

        self.sync_send_peers()

        return response

    def handle_leave_request(self, candidate):
        pass

    def sync_send_peers(self):
        peer_data = self.get_share_peers()

def make_config(storepath=None, MB=100):
    capacity = int(MB * 1e6)
    config = {'namespace':uuid.uuid4().hex,
              'id':uuid.uuid4().hex,
              'capacity':capacity}
    return config

def load_config(filename=None):
    file = open(filename, 'r')
    config = json.loads(file.read())
    return config

def save_config(filename=None, config=None):
    file = open(filename, 'w')
    file.write(json.dumps(config, sort_keys=True, indent=4))
    file.write('\n')

if __name__ == '__main__':
    print('usage: run serverfront.py instead, scallawag.')
