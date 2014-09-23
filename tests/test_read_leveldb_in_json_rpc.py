#encoding:utf8
import json, socket, itertools, struct
import redis
import gevent
import time
from multiprocessing import Pool

class JSONClient(object):

    def __init__(self, addr):
        self.socket = socket.create_connection(addr)
        self.id_counter = itertools.count()

    def __del__(self):
        self.socket.close()

    def call(self, name, params):
        request = dict(id=next(self.id_counter),
                    params=params,
                    method=name)
        request_strs = json.dumps(request).encode()
        raw = struct.pack(">I", len(request_strs))
        self.socket.sendall(raw + request_strs)

        # This must loop if resp is bigger than 4K
        raw_len = self.socket.recv(4)
        package_len = struct.unpack(">I", raw_len)[0]
        raw_message = self.socket.recv(package_len)
        while len(raw_message) < package_len:
            raw_message = raw_message + self.socket.recv(package_len - len(raw_message))
        response = json.loads(raw_message)
        if response.get('id') != request.get('id'):
            raise Exception("expected id=%s, received id=%s: %s"
                            %(request.get('id'), response.get('id'),
                              response.get('error')))

        if response.get('error') is not None:
            raise Exception(response.get('error'))

        return response.get('result')

count = 0
last_ts = time.time()
def Progress():
    global count, last_ts
    count = count + 1
    if count % 100 == 0:
        cur_ts = time.time()
        print("processes %d, speed:%d" % (count, 100/(cur_ts-last_ts)))
        last_ts = cur_ts

def Map(keys):
    rpc = JSONClient(("127.0.0.1", 5200))
    for key in keys:
        rpc.call("Get", key)
        Progress()

config = {}
with open("conf/bench.json") as fh:
    content = fh.read()
    config = json.loads(content)
HOST, PORT = config["redis"]["host"].split(":")
redis_db = redis.StrictRedis(host=HOST, port=int(PORT),db=config["redis"]["db"],password=config["redis"]["password"])
all_keys = redis_db.keys()
begin_timestamp = time.time()
max_procs = 3
parallels_keys = []
step = len(all_keys)/max_procs
for i in range(0, len(all_keys), step):
    parallels_keys.append(all_keys[i:i + step])
print "parallels_keys", len(parallels_keys)
pool = Pool(processes = max_procs)
pool.map(Map, parallels_keys)
end_timestamp = time.time()
print ("end speed:", len(all_keys)/(end_timestamp - begin_timestamp))

