"""
@date: 2021-03-27
@author: liberty
@file: 3.7.2_with_trans

the is a part of "RedisInAction"

"""
import threading
import time

import redis

conn = redis.Redis('192.168.20.123', db=0)


def trans():
    pp = conn.pipeline()
    pp.incr('trans:')
    time.sleep(0.1)
    pp.incr('trans:', -1)
    print(pp.execute())


if __name__ == '__main__':
    while True:
        for i in range(3):
            threading.Thread(target=trans, ).start()
        time.sleep(0.5)
