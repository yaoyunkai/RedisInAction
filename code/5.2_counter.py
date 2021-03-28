"""
@date: 2021-03-28
@author: liberty
@file: 5.2_counter

the is a part of "RedisInAction"

zset中当分值一样时，会按照成员名进行排序。

"""

# 以秒为单位的计数器精度，分别为1秒钟、5秒钟、1分钟、5分钟、1小时、5小时、1天——用户可以按需调整这些精度。
import bisect
import time

import redis

PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]

QUIT = False
SAMPLE_COUNT = 120


def update_counter(conn, name, count=1, now=None):
    now = now or time.time()
    _pipe = conn.pipeline()
    for prec in PRECISION:
        pnow = int(now / prec) * prec
        _hash = '{}:{}'.format(prec, name)

        _pipe.zadd('known:', _hash, 0)
        _pipe.hincrby('count:' + _hash, pnow, count)
    _pipe.execute()


def get_counter(conn, name, precision):
    _hash = '{}:{}'.format(precision, name)
    data = conn.hgetall('count:{}'.format(_hash))
    _res = []

    for k, v in data.iteritems():
        _res.append((int(k), int(v)))

    _res.reverse()
    return _res


def clean_counters(conn):
    pipe = conn.pipeline(True)
    # 为了平等地处理更新频率各不相同的多个计数器，程序需要记录清理操作执行的次数。
    passes = 0
    # 持续地对计数器进行清理，直到退出为止。
    while not QUIT:
        # 记录清理操作开始执行的时间，用于计算清理操作执行的时长。
        start = time.time()
        # 渐进地遍历所有已知的计数器。
        index = 0
        while index < conn.zcard('known:'):
            # 取得被检查计数器的数据。
            thash = conn.zrange('known:', index, index)
            index += 1
            if not thash:
                break
            thash = thash[0]
            # 取得计数器的精度。
            prec = int(thash.partition(':')[0])
            # 因为清理程序每60秒钟就会循环一次，
            # 所以这里需要根据计数器的更新频率来判断是否真的有必要对计数器进行清理。
            bprec = int(prec // 60) or 1
            # 如果这个计数器在这次循环里不需要进行清理，
            # 那么检查下一个计数器。
            # （举个例子，如果清理程序只循环了三次，而计数器的更新频率为每5分钟一次，
            # 那么程序暂时还不需要对这个计数器进行清理。）
            if passes % bprec:
                continue

            hkey = 'count:' + thash
            # 根据给定的精度以及需要保留的样本数量，
            # 计算出我们需要保留什么时间之前的样本。
            cutoff = time.time() - SAMPLE_COUNT * prec
            # 获取样本的开始时间，并将其从字符串转换为整数。
            samples = map(int, conn.hkeys(hkey))
            # 计算出需要移除的样本数量。
            samples.sort()
            remove = bisect.bisect_right(samples, cutoff)

            # 按需移除计数样本。
            if remove:
                conn.hdel(hkey, *samples[:remove])
                # 这个散列可能已经被清空。
                if remove == len(samples):
                    try:
                        # 在尝试修改计数器散列之前，对其进行监视。
                        pipe.watch(hkey)
                        # 验证计数器散列是否为空，如果是的话，
                        # 那么从记录已知计数器的有序集合里面移除它。
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem('known:', thash)
                            pipe.execute()
                            # 在删除了一个计数器的情况下，
                            # 下次循环可以使用与本次循环相同的索引。
                            index -= 1
                        else:
                            # 计数器散列并不为空，
                            # 继续让它留在记录已有计数器的有序集合里面。
                            pipe.unwatch()
                    # 有其他程序向这个计算器散列添加了新的数据，
                    # 它已经不再是空的了，继续让它留在记录已知计数器的有序集合里面。
                    except redis.exceptions.WatchError:
                        pass

        # 为了让清理操作的执行频率与计数器更新的频率保持一致，
        # 对记录循环次数的变量以及记录执行时长的变量进行更新。
        passes += 1
        duration = min(int(time.time() - start) + 1, 60)
        # 如果这次循环未耗尽60秒钟，那么在余下的时间内进行休眠；
        # 如果60秒钟已经耗尽，那么休眠一秒钟以便稍作休息。
        time.sleep(max(60 - duration, 1))


if __name__ == '__main__':
    re = redis.Redis('192.168.20.123', db=5)

    for i in range(10000):
        update_counter(re, 'hits')
