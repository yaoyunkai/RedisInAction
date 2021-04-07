"""
@date: 2021-04-07
@author: liberty
@file: 8.1_user_and_status

the is a part of "RedisInAction"

"""
import http.server
import math
import socketserver
import threading
import time
import uuid


def to_bytes(x):
    return x.encode() if isinstance(x, str) else x


def to_str(x):
    return x.decode() if isinstance(x, bytes) else x


def acquire_lock_with_timeout(conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif conn.ttl(lockname) < 0:
            conn.expire(lockname, lock_timeout)

        time.sleep(.001)

    return False


def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    identifier = to_bytes(identifier)

    while True:
        try:
            pipe.watch(lockname)
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True

            pipe.unwatch()
            break

        except redis.exceptions.WatchError:
            pass

    return False


def execute_later(conn, queue, name, args):
    # this is just for testing purposes
    assert conn is args[0]
    t = threading.Thread(target=globals()[name], args=tuple(args))
    t.setDaemon(True)
    t.start()


def create_user(conn, login, name):
    lower_login = login.lower()
    lock = acquire_lock_with_timeout(conn, 'user:' + lower_login, 1)
    if not lock:
        return None

    if conn.hget('users:', lower_login):
        release_lock(conn, 'user:' + lower_login, lock)
        return None
    uid = conn.incr('user:id:')
    pipe = conn.pipeline(transaction=True)
    # 用hash表 存储用户id 和 登录名 的关系
    pipe.hset('users:', lower_login, uid)
    pipe.hmset('user:{}'.format(uid), {
        'login': login,
        'id': uid,
        'name': name,
        'followers': 0,
        'following': 0,
        'posts': 0,
        'signup': time.time(),
    })
    pipe.execute()
    release_lock(conn, 'user:' + lower_login, lock)
    return uid


def create_status(conn, uid, message, **data):
    pipeline = conn.pipeline(True)
    pipeline.hget('user:%s' % uid, 'login')
    pipeline.incr('status:id:')
    login, status_id = pipeline.execute()

    if not login:
        return None

    data.update({
        'message': message,
        'posted': time.time(),
        'id': status_id,
        'uid': uid,
        'login': login,
    })
    pipeline.hmset('status:%s' % status_id, data)
    pipeline.hincrby('user:%s' % uid, 'posts')  # 增加一条状态计数
    pipeline.execute()
    return status_id


def get_status_messages(conn, uid, timeline='home:', page=1, count=30):
    status_list = conn.zrevrange('{}{}'.format(timeline, uid), (page - 1) * count, page * count - 1)
    pipe = conn.pipeline(True)
    for sid in status_list:
        pipe.hgetall('status:{}'.format(sid))

    return [_f for _f in pipe.execute() if _f]


HOME_TIMELINE_SIZE = 1000


def follow_user(conn, uid, other_uid):
    # conn = redis.Redis()

    fkey1 = 'following:%s' % uid
    fkey2 = 'followers:%s' % other_uid

    if conn.zscore(fkey1, other_uid):
        return None

    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zadd(fkey1, other_uid, now)
    pipeline.zadd(fkey2, uid, now)
    pipeline.zrevrange('profile:{}'.format(other_uid), 0, HOME_TIMELINE_SIZE - 1, withscores=True)

    following, followers, status_and_score = pipeline.execute()[-3:]
    pipeline.hincrby('user:%s' % uid, 'following', int(following))
    pipeline.hincrby('user:%s' % other_uid, 'followers', int(followers))

    if status_and_score:
        pipeline.zadd('home:{}'.format(uid), **dict(status_and_score))
    pipeline.zremrangebyrank('home:{}'.format(uid), 0, -HOME_TIMELINE_SIZE - 1)

    pipeline.execute()
    return True


def unfollow_user(conn, uid, other_uid):
    fkey1 = 'following:%s' % uid
    fkey2 = 'followers:%s' % other_uid

    if not conn.zscore(fkey1, other_uid):
        return None

    pipeline = conn.pipeline(True)
    pipeline.zrem(fkey1, other_uid)
    pipeline.zrem(fkey2, uid)
    pipeline.zrevrange('profile:%s' % other_uid, 0, HOME_TIMELINE_SIZE - 1)  # 取出其他人的消息
    following, followers, statuses = pipeline.execute()[-3:]

    pipeline.hincrby('user:%s' % uid, 'following', -int(following))
    pipeline.hincrby('user:%s' % other_uid, 'followers', -int(followers))
    if statuses:
        pipeline.zrem('home:%s' % uid, *statuses)  # 在我自己的home 里面删除其他人的消息

    pipeline.execute()
    return True


REFILL_USERS_STEP = 50


def refill_timeline(conn, incoming, timeline, start=0):
    """
    
    :param conn: 
    :param incoming: 表示关注着列表 following
    :param timeline: 
    :param start: 
    :return: 
    """

    if not start and conn.zcard(timeline) >= 750:
        return

    # 取出我的关注者列表
    users = conn.zrangebyscore(incoming, start, 'inf', start=0, num=REFILL_USERS_STEP, withscores=True)

    pipeline = conn.pipeline(False)
    for uid, start in users:
        uid = to_str(uid)  # convert to str
        pipeline.zrevrange('profile:%s' % uid, 0, HOME_TIMELINE_SIZE - 1, withscores=True)

    messages = []  # 我关注的人的最近五十条消息
    for results in pipeline.execute():
        messages.extend(results)

    messages.sort(key=lambda x: -x[1])
    del messages[HOME_TIMELINE_SIZE:]  # 只保留所有的消息里面 最近的五十条消息

    pipeline = conn.pipeline(True)
    if messages:
        pipeline.zadd(timeline, dict(messages))  # 将消息添加到我的home:xxx
    pipeline.zremrangebyrank(timeline, 0, -HOME_TIMELINE_SIZE - 1)  # 从home:xxx 移除最久的五十条消息。
    pipeline.execute()

    if len(users) >= REFILL_USERS_STEP:
        execute_later(conn, 'default', 'refill_timeline', [conn, incoming, timeline, start])


def follow_user_list(conn, other_uid, list_id):
    fkey1 = 'list:in:%s' % list_id
    fkey2 = 'list:out:%s' % other_uid
    timeline = 'list:statuses:%s' % list_id

    if conn.zscore(fkey1, other_uid):
        return None

    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zadd(fkey1, {other_uid: now})
    pipeline.zadd(fkey2, {list_id: now})
    pipeline.zrevrange('profile:%s' % other_uid, 0, HOME_TIMELINE_SIZE - 1, withscores=True)
    following, followers, status_and_score = pipeline.execute()[-3:]

    pipeline.hincrby('list:%s' % list_id, 'following', int(following))
    pipeline.zadd(timeline, dict(status_and_score))
    pipeline.zremrangebyrank(timeline, 0, -HOME_TIMELINE_SIZE - 1)

    pipeline.execute()
    return True


def unfollow_user_list(conn, other_uid, list_id):
    fkey1 = 'list:in:%s' % list_id
    fkey2 = 'list:out:%s' % other_uid
    timeline = 'list:statuses:%s' % list_id

    if not conn.zscore(fkey1, other_uid):
        return None

    pipeline = conn.pipeline(True)
    pipeline.zrem(fkey1, other_uid)
    pipeline.zrem(fkey2, list_id)
    pipeline.zrevrange('profile:%s' % other_uid, 0, HOME_TIMELINE_SIZE - 1)
    following, followers, statuses = pipeline.execute()[-3:]

    pipeline.hincrby('list:%s' % list_id, 'following', -int(following))
    if statuses:
        pipeline.zrem(timeline, *statuses)
        refill_timeline(conn, fkey1, timeline)

    pipeline.execute()
    return True


def post_status(conn, uid, message, **data):
    sid = create_status(conn, uid, message, **data)
    if not sid:
        return None

    pub_time = conn.hget('status:{}'.format(sid), 'posted')
    if not pub_time:
        return None

    _post = {str(id): float(pub_time)}
    conn.zadd('profile:{}'.format(uid), **_post)
    syndicate_status(conn, uid, _post)

    return sid


POSTS_PER_PASS = 1000


def syndicate_status(conn, uid, post, start=0):
    followers = conn.zrangebyscore('followers:%s' % uid, start, 'inf', start=0, num=POSTS_PER_PASS, withscores=True)

    pipeline = conn.pipeline(False)
    for follower, start in followers:
        follower = to_str(follower)
        pipeline.zadd('home:%s' % follower, post)
        pipeline.zremrangebyrank('home:%s' % follower, 0, -HOME_TIMELINE_SIZE - 1)
    pipeline.execute()

    if len(followers) >= POSTS_PER_PASS:
        execute_later(conn, 'default', 'syndicate_status', [conn, uid, post, start])


def delete_status(conn, uid, status_id):
    status_id = to_str(status_id)
    key = 'status:%s' % status_id
    lock = acquire_lock_with_timeout(conn, key, 1)
    if not lock:
        return None

    if conn.hget(key, 'uid') != to_bytes(uid):
        release_lock(conn, key, lock)
        return None

    uid = to_str(uid)
    pipeline = conn.pipeline(True)
    pipeline.delete(key)
    pipeline.zrem('profile:%s' % uid, status_id)
    pipeline.zrem('home:%s' % uid, status_id)
    pipeline.hincrby('user:%s' % uid, 'posts', -1)
    pipeline.execute()

    release_lock(conn, key, lock)
    return True


def clean_timelines(conn, uid, status_id, start=0, on_lists=False):
    uid = to_str(uid)
    status_id = to_str(status_id)
    key = 'followers:%s' % uid
    base = 'home:%s'
    if on_lists:
        key = 'list:out:%s' % uid
        base = 'list:statuses:%s'
    followers = conn.zrangebyscore(key, start, 'inf', start=0, num=POSTS_PER_PASS, withscores=True)

    pipeline = conn.pipeline(False)
    for follower, start in followers:
        follower = to_str(follower)
        pipeline.zrem(base % follower, status_id)
    pipeline.execute()

    if len(followers) >= POSTS_PER_PASS:
        execute_later(conn, 'default', 'clean_timelines', [conn, uid, status_id, start, on_lists])

    elif not on_lists:
        execute_later(conn, 'default', 'clean_timelines', [conn, uid, status_id, 0, True])


# =============================================================================
# =============================================================================
# =========================  8.5 Stream API       =============================
# =============================================================================
# =============================================================================

class StreamingAPIServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


class StreamingAPIRequestHandler(http.server.BaseHTTPRequestHandler):
    identifier = None
    query = None


if __name__ == '__main__':
    import redis

    red = redis.Redis('192.168.20.123', db=5)

    # create_user(red, 'liberty', 'Liberty Yao')
    # create_status(red, 1, 'this is start')
