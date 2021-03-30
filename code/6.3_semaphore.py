"""
@date: 2021-03-29
@author: liberty
@file: 6.3_semaphore

the is a part of "RedisInAction"

dataset:

semaphore:xxxx                     zset
uuid                   timestamp
uuid2                  timestamp2


"""
import time
import uuid


def acquire_semaphore(conn, semname, limit, timeout=10):
    """
    不公平：如果两个server 时间不一致就会出现问题

    """
    identifier = str(uuid.uuid4())
    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zadd(semname, {identifier: now})
    pipeline.zrank(semname, identifier)
    if pipeline.execute()[-1] < limit:
        return identifier

    conn.zrem(semname, identifier)
    return None


def release_semaphore(conn, semname, identifier):
    return conn.zrem(semname, identifier)


def acquire_fair_semaphore(conn, semname, limit, timeout=10):
    identifier = str(uuid.uuid4())
    czset = semname + ':owner'
    ctr = semname + ':counter'

    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zinterstore(czset, {czset: 1, semname: 0})

    pipeline.incr(ctr)
    counter = pipeline.execute()[-1]

    pipeline.zadd(semname, {identifier: now})
    pipeline.zadd(czset, {identifier: counter})

    pipeline.zrank(czset, identifier)
    if pipeline.execute()[-1] < limit:
        return identifier

    pipeline.zrem(semname, identifier)
    pipeline.zrem(czset, identifier)
    pipeline.execute()
    return None


def release_fair_semaphore(conn, semname, identifier):
    pipeline = conn.pipeline(True)
    pipeline.zrem(semname, identifier)
    pipeline.zrem(semname + ':owner', identifier)
    return pipeline.execute()[0]


def refresh_fair_semaphore(conn, semname, identifier):
    if conn.zadd(semname, {identifier: time.time()}):
        release_fair_semaphore(conn, semname, identifier)
        return False
    return True
