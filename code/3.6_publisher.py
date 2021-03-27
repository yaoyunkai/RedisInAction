"""
@date: 2021-03-27
@author: liberty
@file: 3.6_publisher

the is a part of "RedisInAction"

"""
import time
import redis

conn = redis.Redis('192.168.20.123', db=0)


def publisher(n):
    time.sleep(1)
    for i in range(n):
        conn.publish('channel', i)
        time.sleep(1)


if __name__ == '__main__':
    publisher(100)
