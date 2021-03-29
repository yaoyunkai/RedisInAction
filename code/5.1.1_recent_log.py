"""
@date: 2021-03-28
@author: liberty
@file: 5.5.1_recent_log

the is a part of "RedisInAction"

"""
import datetime
import logging
import time

SEVERITY = {
    logging.DEBUG: 'debug',
    logging.INFO: 'info',
    logging.WARNING: 'warning',
    logging.ERROR: 'error',
    logging.CRITICAL: 'critical',
    'debug': 'debug',
    'info': 'info',
    'warning': 'warning',
    'error': 'error',
    'critical': 'critical',
}


def log_recent(conn, name, message, severity=logging.INFO, pipe=None):
    severity = str(SEVERITY.get(severity, severity)).lower()
    dest = 'recent:{}:{}'.format(name, severity)
    message = '{} {}'.format(time.asctime(), message)

    pipe = pipe or conn.pipeline()
    pipe.lpush(dest, message)
    pipe.ltrim(dest, 0, 99)
    pipe.execute()


def log_common(conn, name, message, severity=logging.INFO, timeout=5):
    # 设置日志的级别。
    severity = str(SEVERITY.get(severity, severity)).lower()
    # 负责存储最新日志的键。
    destination = 'common:%s:%s' % (name, severity)
    # 因为程序每小时需要轮换一次日志，所以它使用一个键来记录当前所处的小时数。
    start_key = destination + ':start'
    pipe = conn.pipeline()
    end = time.time() + timeout
    while time.time() < end:
        try:
            # 对记录当前小时数的键进行监视，确保轮换操作可以正确地执行。
            pipe.watch(start_key)
            # 取得当前时间。
            now = datetime.datetime.utcnow().timetuple()
            # 取得当前所处的小时数。
            hour_start = datetime.datetime(*now[:4]).isoformat()

            existing = pipe.get(start_key)
            # 创建一个事务。
            pipe.multi()
            # 如果目前的常见日志列表是上一个小时的……
            if existing and existing < hour_start:
                # ……那么将旧的常见日志信息进行归档。
                pipe.rename(destination, destination + ':last')
                pipe.rename(start_key, destination + ':pstart')
                # 更新当前所处的小时数。
                pipe.set(start_key, hour_start)

            # 对记录日志出现次数的计数器执行自增操作。
            pipe.zincrby(destination, message)
            # log_recent()函数负责记录日志并调用execute()函数。
            log_recent(pipe, name, message, severity, pipe)  # NOQA
            return
        except redis.exceptions.WatchError:
            # 如果程序因为其他客户端在执行归档操作而出现监视错误，那么重试。
            continue


if __name__ == '__main__':
    import faker
    import redis

    re = redis.Redis('192.168.20.123')
    _pipe = re.pipeline(False)

    f = faker.Faker()

    for i in range(50):
        msg = f.sentence(nb_words=6, variable_nb_words=True, ext_word_list=None)
        log_recent(re, 'demo', msg, pipe=_pipe)
