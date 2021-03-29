"""
@date: 2021-03-29
@author: liberty
@file: 5.2.2_statistical

the is a part of "RedisInAction"

"""
import datetime
import time
import uuid


def update_stats(conn, context, _type, value, timeout=5):
    # 设置用于存储统计数据的键。
    destination = 'stats:%s:%s' % (context, _type)
    # 像common_log()函数一样，
    # 处理当前这一个小时的数据和上一个小时的数据。
    start_key = destination + ':start'
    pipe = conn.pipeline(True)
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.datetime.utcnow().timetuple()
            hour_start = datetime.datetime(*now[:4]).isoformat()

            existing = pipe.get(start_key)
            pipe.multi()

            if not existing:
                pipe.set(start_key, hour_start)
            elif existing < hour_start:
                pipe.rename(destination, destination + ':last')
                pipe.rename(start_key, destination + ':pstart')
                pipe.set(start_key, hour_start)

            tkey1 = str(uuid.uuid4())
            tkey2 = str(uuid.uuid4())
            # 将值添加到临时键里面。
            pipe.zadd(tkey1, 'min', value)
            pipe.zadd(tkey2, 'max', value)
            # 使用合适聚合函数MIN和MAX，
            # 对存储统计数据的键和两个临时键进行并集计算。
            pipe.zunionstore(destination, [destination, tkey1], aggregate='min')
            pipe.zunionstore(destination, [destination, tkey2], aggregate='max')

            # 删除临时键。
            pipe.delete(tkey1, tkey2)
            # 对有序集合中的样本数量、值的和、值的平方之和三个成员进行更新。
            pipe.zincrby(destination, 'count')
            pipe.zincrby(destination, 'sum', value)
            pipe.zincrby(destination, 'sumsq', value * value)

            # 返回基本的计数信息，以便函数调用者在有需要时做进一步的处理。
            return pipe.execute()[-3:]
        except Exception:
            # 如果新的一个小时已经开始，并且旧的数据已经被归档，那么进行重试。
            continue


def get_stats(conn, context, type):
    # 程序将从这个键里面取出统计数据。
    key = 'stats:%s:%s' % (context, type)
    # 获取基本的统计数据，并将它们都放到一个字典里面。
    data = dict(conn.zrange(key, 0, -1, withscores=True))
    # 计算平均值。
    data['average'] = data['sum'] / data['count']
    # 计算标准差的第一个步骤。
    numerator = data['sumsq'] - data['sum'] ** 2 / data['count']
    # 完成标准差的计算工作。
    data['stddev'] = (numerator / (data['count'] - 1 or 1)) ** .5
    return data


# @contextlib.contextmanager
# def access_time(conn, context):
#     # 记录代码块执行前的时间。
#     start = time.time()
#     # 运行被包裹的代码块。
#     yield
#
#     # 计算代码块的执行时长。
#     delta = time.time() - start
#     # 更新这一上下文的统计数据。
#     stats = update_stats(conn, context, 'AccessTime', delta)
#     # 计算页面的平均访问时长。
#     average = stats[1] / stats[0]
#
#     pipe = conn.pipeline(True)
#     # 将页面的平均访问时长添加到记录最慢访问时间的有序集合里面。
#     pipe.zadd('slowest:AccessTime', context, average)
#     # AccessTime有序集合只会保留最慢的100条记录。
#     pipe.zremrangebyrank('slowest:AccessTime', 0, -101)
#     pipe.execute()
#
#
# def process_view(conn, callback):
#     # 计算并记录访问时长的上下文管理器就是这样包围代码块的。
#     with access_time(conn, request.path):
#         # 当上下文管理器中的yield语句被执行时，这个语句就会被执行。
#         return callback()
#

if __name__ == '__main__':
    import redis
    import random

    re = redis.Redis('192.168.20.123', db=6, decode_responses=True)

    for i in range(1000):
        update_stats(re, 'Page', 'AccessTime', random.random())

    # print(get_stats(re, 'Page', 'AccessTime'))
