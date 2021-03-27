"""
@date: 2021-03-27
@author: liberty
@file: 3.6_sub

the is a part of "RedisInAction"

"""
import threading

import redis

conn = redis.Redis('192.168.20.123', db=0)


def run_sub():
    pubusb = conn.pubsub()
    pubusb.subscribe('channel')
    for item in pubusb.listen():
        print(item)

    pubusb.unsubscribe()


if __name__ == '__main__':
    run_sub()
