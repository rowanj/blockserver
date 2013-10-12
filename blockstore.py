import hashlib
import os
import errno
import zlib
import lockfile

class BlockstoreError(Exception):
    pass

class BadDataError(BlockstoreError):
    pass

class BadTypeError(BlockstoreError):
    pass

class NoDataError(BlockstoreError):
    def __init__(self, hash):
        self.hash = hash

class Blockstore:    
    def __init__(self, storepath):
        self.storepath = os.path.realpath(storepath)
        try:
            os.makedirs(self.storepath)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        self.objectspath = os.path.join(self.storepath, 'objects')
        self.lockfile = lockfile.FileLock(os.path.join(self.storepath, 'lockfile'))

    def store(self, type='blob', data=None):
        if not type:
            raise BadTypeError()

        if not data:
            raise NoDataError()

        length = len(data)
        header = type + ' ' + str(length) + '\0'
        store = header.encode() + data

        hasher = hashlib.sha1()
        hasher.update(store)
        hash = hasher.hexdigest()
        result = {'hash':hash, 'length':length, 'type':type}
        with self.lockfile:
            if self.is_hash_stored(hash):
                result['status'] = 'already stored'
                return result

            loose_dir = self.dir_for_loose_object(hash)
            try:
                os.makedirs(loose_dir)
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise

            filepath = self.path_for_loose_object(hash)
            tempfilename = filepath + '.tmp'
            f = open(tempfilename, 'wb')
            
            compressed = zlib.compress(store)
            f.write(compressed)
            f.close()

            os.rename(tempfilename, filepath)
            result['status'] = 'added'
            return result
        
    def is_hash_stored(self, hash):
        filepath = self.path_for_loose_object(hash)
        if os.path.exists(filepath):
            return True
        return False

    def dir_for_loose_object(self, hash):
        hashprefix = hash[0:2]
        hashprefixdir = os.path.join(self.objectspath, hashprefix)
        return hashprefixdir

    def path_for_loose_object(self, hash):
        dir = self.dir_for_loose_object(hash)
        filename = hash[2:]
        path = os.path.join(dir, filename)
        return path

    def retrieve(self, hash):
        with self.lockfile:
            filepath = self.path_for_loose_object(hash)
            try:
                file = open(filepath, 'rb')
            except FileNotFoundError as exc:
                raise NoDataError(hash) from exc
            store = zlib.decompress(file.read())
            delimeterpos = store.find(0)
            type, length = store[0:delimeterpos].decode().split(' ')
            length = int(length)
            bytes = store[delimeterpos + 1:]
            if len(bytes) != length:
                raise BadDataError()
            return {'type':type, 'length':length, 'data':bytes, 'hash':hash}

def test():
    storepath = os.path.join(os.path.dirname(__file__), 'teststore')
    bs = Blockstore(storepath)
    store = bs.store(data="jazaaam".encode())
    print("stored", store)
    res = bs.retrieve(store['hash'])
    print("retrieved", res)
    bs.retrieve('0889')

if __name__ == "__main__":
    test()
