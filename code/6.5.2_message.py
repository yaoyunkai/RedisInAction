"""
@date: 2021-03-30
@author: liberty
@file: 6.5.2_message

the is a part of "RedisInAction"


chat:827           zset
user1         45
user2         55


seen:jason22        zset
827           445

"""
import json
import time
import uuid


def create_chat(conn, sender, recipients, message, chat_id=None):
    chat_id = chat_id or str(conn.incr('ids:chat:'))

    recipients.append(sender)
    recipients_dict = dict((r, 0) for r in recipients)

    pipe = conn.pipeline(True)
    pipe.zadd('chat:' + chat_id, **recipients_dict)

    for rec in recipients_dict:
        pipe.zadd('seen:' + rec, chat_id, 0)
    pipe.execute()

    return send_message(conn, chat_id, sender, message)


def send_message(conn, chat_id, sender, message):
    iden = acquire_lock(conn, 'chat:' + chat_id)
    if not iden:
        raise Exception('clound not get lock')
    try:
        mid = conn.incr('ids:' + chat_id)
        ts = time.time()
        packed = {
            'id': mid,
            'ts': ts,
            'message': message,
            'sender': sender
        }
        conn.zadd('msgs:' + chat_id, packed, mid)
    finally:
        release_lock(conn, 'chat:' + chat_id, iden)


def fetch_pending_messages(conn, recipient):
    seen = conn.zrange('seen:' + recipient, 0, -1, withscores=True)

    pipeline = conn.pipeline(True)

    for chat_id, seen_id in seen:
        # +1 是因为消息id已读了
        pipeline.zrangebyscore(b'msgs:' + chat_id, seen_id + 1, 'inf')
    chat_info = list(zip(seen, pipeline.execute()))

    for i, ((chat_id, seen_id), messages) in enumerate(chat_info):
        if not messages:
            continue
        messages[:] = list(map(json.loads, messages))
        seen_id = messages[-1]['id']
        conn.zadd(b'chat:' + chat_id, {recipient: seen_id})

        min_id = conn.zrange(b'chat:' + chat_id, 0, 0, withscores=True)

        pipeline.zadd('seen:' + recipient, {chat_id: seen_id})
        if min_id:
            pipeline.zremrangebyscore(b'msgs:' + chat_id, 0, min_id[0][1])
        chat_info[i] = (chat_id, messages)
    pipeline.execute()

    return chat_info


def join_chat(conn, chat_id, user):
    message_id = int(conn.get('ids:' + chat_id))

    pipeline = conn.pipeline(True)
    pipeline.zadd('chat:' + chat_id, {user: message_id})
    pipeline.zadd('seen:' + user, {chat_id: message_id})
    pipeline.execute()


def leave_chat(conn, chat_id, user):
    pipeline = conn.pipeline(True)
    pipeline.zrem('chat:' + chat_id, user)
    pipeline.zrem('seen:' + user, chat_id)
    pipeline.zcard('chat:' + chat_id)

    if not pipeline.execute()[-1]:
        pipeline.delete('msgs:' + chat_id)
        pipeline.delete('ids:' + chat_id)
        pipeline.execute()
    else:
        oldest = conn.zrange('chat:' + chat_id, 0, 0, withscores=True)
        conn.zremrangebyscore('msgs:' + chat_id, 0, oldest[0][1])


def acquire_lock(conn, lockname, acquire_timeout=10):
    identifier = str(uuid.uuid4())

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx('lock:' + lockname, identifier):
            return identifier

        time.sleep(.001)

    return False


def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    if isinstance(identifier, str):
        identifier = identifier.encode()

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

        except Exception:
            pass

    return False
