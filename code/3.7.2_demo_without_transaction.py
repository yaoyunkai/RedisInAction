"""
@date: 2021-03-27
@author: liberty
@file: 3.7.2_demo_without_transaction

the is a part of "RedisInAction"

"""
import threading
import time

import redis

conn = redis.Redis('192.168.20.123', db=0)


def notrans():
    print(conn.incr('notrans:'))
    time.sleep(0.1)
    conn.incr('notrans:', -1)


if __name__ == '__main__':
    while True:
        for i in range(3):
            threading.Thread(target=notrans, ).start()
        time.sleep(0.5)
