"""
@date: 2021-03-30
@author: liberty
@file: 7.1_searching

the is a part of "RedisInAction"

"""
import re
import uuid

STOP_WORDS = set('''able about across after all almost also am among
an and any are as at be because been but by can cannot could dear did
do does either else ever every for from get got had has have he her
hers him his how however if in into is it its just least let like
likely may me might most must my neither no nor not of off often on
only or other our own rather said say says she should since so some
than that the their them then there these they this tis to too twas us
wants was we were what when where which while who whom why will with
would yet you your'''.split())

WORDS_RE = re.compile("[a-z']{2,}")


def tokenize(content):
    """
    从文档里提取单词的过程通常被称为语法分析 parsing 和 标记化 tokenization
    标记化的一个常见的附加步骤，就是移除内容中的非用词 stop word。

    :param content: 文章的内容
    :return:
    """
    words = set()
    for match in WORDS_RE.finditer(content.lower()):
        word = match.group().strip("'")
        if len(word) >= 2:
            words.add(word)
    return words - STOP_WORDS


def index_document(conn, doc_id, content):
    """
    保存集合的过程

    :param conn:
    :param doc_id: document id
    :param content: document content
    :return:
    """
    words = tokenize(content)

    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd('idx:' + word, doc_id)
    return len(pipeline.execute())


def _set_common(conn, method, names, ttl=30, execute=True):
    """
    将结果保存到 idx:uuid4 --> set

    :param conn:
    :param method: 使用方法 处理不同集合的方法
    :param names: 给定的单词，即要选择的idx:[key] 的key
    :param ttl:
    :param execute:
    :return:
    """
    _id = str(uuid.uuid4())
    pipeline = conn.pipeline(True) if execute else conn
    names = ['idx:' + name for name in names]
    getattr(pipeline, method)('idx:' + _id, *names)
    pipeline.expire('idx:' + _id, ttl)
    if execute:
        pipeline.execute()
    return _id


def intersect(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sinterstore', items, ttl, _execute)


def union(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sunionstore', items, ttl, _execute)


def difference(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sdiffstore', items, ttl, _execute)


QUERY_RE = re.compile("[+-]?[a-z']{2,}")


def parse(query):
    """
    解析查询条件

    :param query:
    :return:
    """
    unwanted = set()
    _all = []
    current = set()
    for match in QUERY_RE.finditer(query.lower()):
        word = match.group()
        prefix = word[:1]
        if prefix in '+-':
            word = word[1:]
        else:
            prefix = None
        word = word.strip("'")
        if len(word) < 2 or word in STOP_WORDS:
            continue

        if prefix == '-':
            unwanted.add(word)
            continue

        if current and not prefix:
            _all.append(list(current))
            current = set()
        current.add(word)

    if current:
        _all.append(list(current))

    return _all, list(unwanted)


def parse_and_search(conn, query, ttl=30):
    """

    :param conn:
    :param query: 输入的查询字符串
    :param ttl:
    :return:
    """
    _all, unwanted = parse(query)
    if not _all:
        return None

    to_intersect = []
    for syn in _all:
        if len(syn) > 1:
            to_intersect.append(union(conn, syn, ttl=ttl))
        else:
            to_intersect.append(syn[0])

    if len(to_intersect) > 1:
        intersect_result = intersect(conn, to_intersect, ttl=ttl)
    else:
        intersect_result = to_intersect[0]

    if unwanted:
        unwanted.insert(0, intersect_result)
        return difference(conn, unwanted, ttl=ttl)

    return intersect_result


def search_and_sort(conn, query, tid=None, ttl=300, sort="-updated", start=0, num=20):
    """
    Redis SORT 语法,

    :param conn:
    :param query:
    :param tid: uuid4
    :param ttl:
    :param sort:
    :param start: idx:uuid4 结果集中 开始的位置
    :param num:
    :return:
    """
    desc = sort.startswith('-')
    sort = sort.lstrip('-')
    by = "kb:doc:*->" + sort
    alpha = sort not in ('updated', 'id', 'created')

    if tid and not conn.expire(tid, ttl):
        tid = None

    if not tid:
        tid = parse_and_search(conn, query, ttl=ttl)

    pipeline = conn.pipeline(True)
    pipeline.scard('idx:' + tid)
    pipeline.sort('idx:' + tid, by=by, alpha=alpha, desc=desc, start=start, num=num)
    results = pipeline.execute()

    return results[0], results[1], tid


def search_and_zsort(conn, query, uniq_id=None, ttl=300, update=1, vote=0, start=0, num=20, desc=True):
    if uniq_id and not conn.expire(uniq_id, ttl):
        uniq_id = None

    if not uniq_id:
        uniq_id = parse_and_search(conn, query, ttl=ttl)

        scored_search = {
            uniq_id: 0,
            'sort:update': update,
            'sort:votes': vote,
        }

        uniq_id = zintersect(conn, scored_search, ttl)

    pipeline = conn.pipeline(True)
    pipeline.zcard('idx:' + uniq_id)
    if desc:
        pipeline.zrevrange('idx:' + uniq_id, start, start + num - 1)
    else:
        pipeline.zrange('idx:' + uniq_id, start, start + num - 1)
    results = pipeline.execute()

    return results[0], results[1], uniq_id


def _zset_common(conn, method, scores, ttl=30, **kw):
    """

    :param conn:
    :param method:
    :param scores: from scored_search --> dict
    :param ttl:
    :param kw:
    :return:
    """
    uniq_id = str(uuid.uuid4())
    execute = kw.pop('_execute', True)
    pipeline = conn.pipeline(True) if execute else conn
    for key in list(scores.keys()):
        scores['idx:' + key] = scores.pop(key)
    # {'idx:uuid4': 0, 'idx:sort:update': 1, 'idx:sort:votes': 0}
    getattr(pipeline, method)('idx:' + uniq_id, scores, **kw)
    pipeline.expire('idx:' + uniq_id, ttl)
    if execute:
        pipeline.execute()
    return uniq_id


def zintersect(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zinterstore', dict(items), ttl, **kw)


def zunion(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zunionstore', dict(items), ttl, **kw)


def string_to_score(string, ignore_case=False):
    if ignore_case:
        string = string.lower()

    pieces = list(map(ord, string[:6]))
    while len(pieces) < 6:
        pieces.append(-1)

    score = 0
    for piece in pieces:
        score = score * 257 + piece + 1

    return score * 2 + (len(string) > 6)


# ================================== 7.3 ================================================

AVERAGE_PER_1K = {}


def cpc_to_ecpm(views, clicks, cpc):
    return 1000. * cpc * clicks / views


def cpa_to_ecpm(views, actions, cpa):
    return 1000. * cpa * actions / views


TO_ECPM = {
    b'cpc': cpc_to_ecpm,
    b'cpa': cpa_to_ecpm,
    b'cpm': lambda *args: args[-1],
}


def index_ad(conn, ad_id, locations, content, ad_type, value):
    pipeline = conn.pipeline(True)
    if not isinstance(ad_type, bytes):
        ad_type = ad_type.encode('utf8')

    for location in locations:
        pipeline.sadd('idx:req:' + location, ad_id)

    words = tokenize(content)
    for word in words:
        # 添加一个有序集合 成员为广告id 分值 0
        pipeline.zadd('idx:' + word, {ad_id: 0})

    rvalue = TO_ECPM[ad_type](1000, AVERAGE_PER_1K.get(ad_type, 1), value)

    # 更新广告的类型
    pipeline.hset('type:', ad_id, ad_type)
    # 添加广告的价值到 有序集合
    pipeline.zadd('idx:ad:value:', {ad_id: rvalue})
    pipeline.zadd('ad:base_value:', {ad_id: value})  # 广告的基本价格
    pipeline.sadd('terms:' + ad_id, *list(words))  # 广告类型的相关性 set
    pipeline.execute()
