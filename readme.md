# Redis In Action

> Written by Liberty

## 1. 初始redis ##

### 1.2 redis 数据结构简介 ###

redis中有五种数据结构: `STRING` `LIST` `SET` `HASH` `ZSET`

#### 1.2.1 redis string ####

| command | behavior |
| :------ | -------- |
| GET     |          |
| SET     |          |
| DEL     |          |

```bash
127.0.0.1:6379> set hello world
OK
127.0.0.1:6379> get hello
"world"
127.0.0.1:6379> del hello
(integer) 1
127.0.0.1:6379> get hello
(nil)
127.0.0.1:6379> 
```

#### 1.2.2 redis list ####

一个列表可以有序地存储多个字符串。

| Command | behavior                                       |
| ------- | ---------------------------------------------- |
| RPUSH   | 将给定值推入列表的右端                         |
| LRANGE  | 获取列表在给定范围上的值 ，`-1` 表示列表的最后 |
| LINDEX  | get element by the index                       |
| LPOP    | 从列表的左边弹出值，并返回被弹出的值           |

```bash
127.0.0.1:6379> rpush list-key item
(integer) 1
127.0.0.1:6379> rpush list-key item2
(integer) 2
127.0.0.1:6379> rpush list-key item2
(integer) 3
127.0.0.1:6379> rpush list-key item
(integer) 4
127.0.0.1:6379> lrange list-key 0 -1
1) "item"
2) "item2"
3) "item2"
4) "item"
127.0.0.1:6379> LINDEX list-key 1
"item2"
127.0.0.1:6379> LINDEX list-key 44
(nil)
127.0.0.1:6379> LPOP list-key
"item"
127.0.0.1:6379> lrange list-key 0 -1
1) "item2"
2) "item2"
3) "item"
127.0.0.1:6379> 
```

#### 1.2.3 reids set （集合） ####

合集保证值的唯一性，类似于python中的SET

其中 redis set 是 unordered 无序的。

| command   | behavior                    |
| --------- | --------------------------- |
| SADD      | add element to set          |
| SMEMBERS  | get all of the set elements |
| SISMEMBER | 判断给定元素是否在集合中    |
| SREM      | 移除集合中存在的某个元素    |

```bash
127.0.0.1:6379> sadd set1 a
(integer) 1
127.0.0.1:6379> sadd set1 b
(integer) 1
127.0.0.1:6379> sadd set1 c
(integer) 1
127.0.0.1:6379> sadd set1 b
(integer) 0
127.0.0.1:6379> 0
(error) ERR unknown command `0`, with args beginning with: 
127.0.0.1:6379> SADD set1 1
(integer) 1
127.0.0.1:6379> SADD set1 "1"
(integer) 0
127.0.0.1:6379> SMEMBERS set1
1) "1"
2) "c"
3) "b"
4) "a"
127.0.0.1:6379> SISMEMBER set1 5
(integer) 0
127.0.0.1:6379> SISMEMBER set1 1
(integer) 1
127.0.0.1:6379> srem set1 1
(integer) 1
127.0.0.1:6379> srem set1 1
(integer) 0
127.0.0.1:6379> 
```

#### 1.2.4 redis hash ####

类似于python的dict ，其中 key可以是数字，而且可以执行自增或自减操作。

| command | behavior                          |
| ------- | --------------------------------- |
| hset    | 在指定的hash表中 加入k-v键值对    |
| hget    | 在指定的hash表中 根据k获取v       |
| hgetall | 获取指定hash表中 所有键值对       |
| hdel    | 在指定的hash表中 移除如果存在的键 |

```bash
127.0.0.1:6379> hset hash1 k1 v1
(integer) 1
127.0.0.1:6379> hset hash1 k2 v2
(integer) 1
127.0.0.1:6379> hset hash1 k1 v3
(integer) 0
127.0.0.1:6379> hset hash1 k3 v3
(integer) 1
127.0.0.1:6379> HGETALL hash1
1) "k1"
2) "v3"
3) "k2"
4) "v2"
5) "k3"
6) "v3"
127.0.0.1:6379> HDEL hash1 k1
(integer) 1
127.0.0.1:6379> HDEL hash1 k2
(integer) 1
127.0.0.1:6379> HGETALL hash1
1) "k3"
2) "v3"
127.0.0.1:6379> 
```

#### 1.2.5 redis ZSET ####

有序集合：键值对，值是分值(score)。

| command       | behavior                           |
| ------------- | ---------------------------------- |
| zadd          | 将一个带分值的key 添加到指定的zset |
| zrange        | index索引，根据索引获取元素        |
| zrangebyscore | 根据分值获取元素                   |

```bash
127.0.0.1:6379> zadd demo 123 k1
(integer) 1
127.0.0.1:6379> zadd demo 125 k2
(integer) 1
127.0.0.1:6379> zadd demo 125 k3
(integer) 1
127.0.0.1:6379> zadd demo 190 k4
(integer) 1
127.0.0.1:6379> ZRANGE demo 0 -1
1) "k1"
2) "k2"
3) "k3"
4) "k4"
127.0.0.1:6379> ZRANGE demo 0 -1 withscores
1) "k1"
2) "123"
3) "k2"
4) "125"
5) "k3"
6) "125"
7) "k4"
8) "190"
127.0.0.1:6379> zrangebysocre demo 0 800 withscores
(error) ERR unknown command `zrangebysocre`, with args beginning with: `demo`, `0`, `800`, `withscores`, 
127.0.0.1:6379> ZRANGEBYSCORE demo  150 190 withscores
1) "k4"
2) "190"
127.0.0.1:6379> zrem demo k2
(integer) 1
127.0.0.1:6379> ZRANGE demo 0 -1 withscores
1) "k1"
2) "123"
3) "k3"
4) "125"
5) "k4"
6) "190"
127.0.0.1:6379> 
```

### 1.3 hello redis ###

一个对文章进行评分排序的例子：

**数据表结构**

- 文章表 hash ---- articlle:id {title: , content: }
- 文章发布时间表 zset ---- time: {article:id, }
- 文章评分表 zset ---- score: {article:id, }
- 用户-文章投票表 set ---- voted:article_id {user:id, }

评分计算规则：票数*常量 + 发布时间

```python
ONE_WEEK_IN_SECONDS = 7 * 86400
# 86400 / 200
VOTE_SCORE = 432
ARTICLES_PER_PAGE = 25


def article_vote(conn, user, article):
    """

    :param conn: redis connection
    :param user: like this: user:<id>
    :param article: like this: article:<id>
    :return: Nothing
    """
    cutoff = time.time() - ONE_WEEK_IN_SECONDS

    if conn.zscore('time:', article) < cutoff:
        return

    article_id = article.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user):
        conn.zincrby('score:', article, VOTE_SCORE)
        conn.hincrby(article, 'votes', 1)


def post_article(conn, user, title, link):
    # 生成一个新的文章ID。
    article_id = str(conn.incr('article:'))

    voted = 'voted:' + article_id
    # 将发布文章的用户添加到文章的已投票用户名单里面，
    # 然后将这个名单的过期时间设置为一周（第3章将对过期时间作更详细的介绍）。
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    now = time.time()
    article = 'article:' + article_id
    # 将文章信息存储到一个散列里面。
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1,
    })

    # 将文章添加到根据发布时间排序的有序集合和根据评分排序的有序集合里面。
    conn.zadd('score:', article, now + VOTE_SCORE)
    conn.zadd('time:', article, now)

    return article_id


def get_articles(conn, page, order='score:'):
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    # 获取多个文章ID。
    ids = conn.zrevrange(order, start, end)
    articles = []
    # 根据文章ID获取文章的详细信息。
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)

    return articles


def add_remove_groups(conn, article_id, to_add=None, to_remove=None):
    if to_add is None:
        to_add = []
    if to_remove is None:
        to_remove = []
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)


def get_group_articles(conn, group, page, order='score:'):
    # 为每个群组的每种排列顺序都创建一个键。
    key = order + group
    # 检查是否有已缓存的排序结果，如果没有的话就现在进行排序。
    if not conn.exists(key):
        # 根据评分或者发布时间，对群组文章进行排序。
        conn.zinterstore(key, ['group:' + group, order], aggregate='max', )
        # 让Redis在60秒钟之后自动删除这个有序集合。
        conn.expire(key, 60)
    # 调用之前定义的get_articles()函数来进行分页并获取文章数据。
    return get_articles(conn, page, key)
```

## 3. redis 命令 ##

公共的命令 set get del , 其中set会改变已有key的数据格式

### 3.1 字符串 ###

可以存储：byte string , 整数 和 浮点数

```bash
127.0.0.1:6379> set k1 1.3
OK
127.0.0.1:6379> set k2 34
OK
127.0.0.1:6379> set k3 5f
OK
127.0.0.1:6379> INCR k1
(error) ERR value is not an integer or out of range
127.0.0.1:6379> INCR k2
(integer) 35
127.0.0.1:6379> INCRBYFLOAT k1 
(error) ERR wrong number of arguments for 'incrbyfloat' command
127.0.0.1:6379> INCRBYFLOAT k1 3
"4.3"
127.0.0.1:6379> INCRBYFLOAT k1 3.5
"7.8"
127.0.0.1:6379> incrby k2 34
(integer) 69
127.0.0.1:6379> incrby k3 sd
(error) ERR value is not an integer or out of range
127.0.0.1:6379> 
```

- 空串或者不存在的键 当作0处理
- 无法被解释为整数的键执行自增自减操作，会报错
- 对于浮点数，只能使用 `INCRBYFLOAT`

| command     | desc                                                         |
| ----------- | ------------------------------------------------------------ |
| incr        |                                                              |
| decr        |                                                              |
| incrby      | incrby key-name amount                                       |
| decrby      |                                                              |
| incrbyfloat |                                                              |
| append      | 将value加到key的值的最后                                     |
| getrange    | 获取一个从[start, end] 的字串                                |
| setrange    | 从start 开始开始的子串设置为给定值                           |
| getbit      | 先将字符串看成bytes，再获取对应哪一位的0或者1                |
| setbit      | 对于某一位设置0或者1，返回之前的值0或者1                     |
| bitcount    | 统计二进制里面1的个数，可选偏移: [start, end]                |
| bitop       | bitop [and\|or\|xor\|not] dest-key ke1,key2,... 返回操作了的key的数量 |

字符串 --> binary：

- ascii 编码的长八位
- utf8长度不确定，从16 - 24 位不等

```bash
127.0.0.1:6379[1]> set k1 abc
OK
127.0.0.1:6379[1]> setbit k1 16 1
(integer) 0
127.0.0.1:6379[1]> get k1
"ab\xe3"
127.0.0.1:6379[1]> setbit k1 16 0
(integer) 1
127.0.0.1:6379[1]> get k1
"abc"
127.0.0.1:6379[1]> set k1 123
OK
127.0.0.1:6379[1]> set k2 456
OK
127.0.0.1:6379[1]> getbit k1 0
(integer) 0
127.0.0.1:6379[1]> getbit k1 1
(integer) 0
127.0.0.1:6379[1]> getbit k1 7
(integer) 1
127.0.0.1:6379[1]> getbit k1 6
(integer) 0
127.0.0.1:6379[1]> bitop and k5 k1 k2
(integer) 3
127.0.0.1:6379[1]> 
```

### 3.2 list ###

列表的常见命令:

| command       | desc                                                   |
| ------------- | ------------------------------------------------------ |
| lpush         | 支持多个value                                          |
| rpush         |                                                        |
| rpop / lpop   |                                                        |
| lindex        |                                                        |
| lrange        | key-name start end 包含start 和 end                    |
| ltrim         | key-name start end 只保留 从start 到 end 范围内的元素. |
| blpop / brpop | key-name timeout 阻塞多少秒 并弹出                     |
| rpoplpush     |                                                        |
| brpoplpush    |                                                        |

```bash
127.0.0.1:6379> RPOP k1
(nil)
127.0.0.1:6379> lpush k1 23 32
(integer) 2
127.0.0.1:6379> rpop k2 aasfd sadfswe wqerqw wqerwqerqw wqer qwer
(error) ERR wrong number of arguments for 'rpop' command
127.0.0.1:6379> rpush k2 aasfd sadfswe wqerqw wqerwqerqw wqer qwer
(integer) 6
127.0.0.1:6379> LRANGE k2 0 -1
1) "aasfd"
2) "sadfswe"
3) "wqerqw"
4) "wqerwqerqw"
5) "wqer"
6) "qwer"
127.0.0.1:6379> LRANGE k2 0 1
1) "aasfd"
2) "sadfswe"
127.0.0.1:6379> LRANGE k2 0 3
1) "aasfd"
2) "sadfswe"
3) "wqerqw"
4) "wqerwqerqw"
127.0.0.1:6379> LRANGE k2 0 2
1) "aasfd"
2) "sadfswe"
3) "wqerqw"
127.0.0.1:6379> LRANGE k2 1
(error) ERR wrong number of arguments for 'lrange' command
127.0.0.1:6379> LRANGE k2 1 1
1) "sadfswe"
127.0.0.1:6379> LTRIM k2 0 1
OK
127.0.0.1:6379> LRANGE k2 0 -1
1) "aasfd"
2) "sadfswe"
127.0.0.1:6379> 
```

### 3.3 set ###

已无序的方式存储多个不同的元素。

| command        | desc                                                         |
| -------------- | ------------------------------------------------------------ |
| sadd           | 支持多个value                                                |
| srem           | 移除里面的元素  支持多个value                                |
| sismember      | 是否存在                                                     |
| scard          | 元素的个数                                                   |
| smembers       | 返回所有的元素                                               |
| srandmmeber    | key-name [count] count为正时，返回的多个元素不会重复         |
| spop           | 随机移除一个元素并返回                                       |
| smove          | source-key dest-key item                                     |
| sdiff 差集运算 | key-name, key-name1, key-name2 返回存在于第一个而不存在与其他的元素 |
| sdiffstore     | dest-key key, key1 key2 ..... 将结果存储到dest-key 返回保存的个数 |
| sinter 交集    |                                                              |
|                |                                                              |
| sunion 并集    |                                                              |
|                |                                                              |

```bash
127.0.0.1:6379> sadd k1 a b c d e f 
(integer) 6
127.0.0.1:6379> srem k1 r
(integer) 0
127.0.0.1:6379> srem k1 a
(integer) 1
127.0.0.1:6379> SCARD k1
(integer) 5
127.0.0.1:6379> SMEMBERS k1
1) "f"
2) "e"
3) "c"
4) "b"
5) "d"
127.0.0.1:6379> smove k1 k2 a
(integer) 0
127.0.0.1:6379> smove k1 k2 b
(integer) 1
127.0.0.1:6379> SMEMBERS k2
1) "b"
127.0.0.1:6379> 

127.0.0.1:6379> sdiff k1
1) "d"
2) "e"
3) "c"
4) "f"
127.0.0.1:6379> sdiff k2
1) "d"
2) "b"
3) "c"
127.0.0.1:6379> sdiff k1 k2
1) "f"
2) "e"
127.0.0.1:6379> 

```

### 3.4 hash ###

| Command      | Desc                      |
| ------------ | ------------------------- |
| hmget        | key-name key key1 key2    |
| hmset        | key-name k1 v1 k2 v2      |
| hdel         | key-name k1 k2            |
| hlen         | key-name 键的数量         |
| hexists      | key-name k1 判断k是否存在 |
| hkeys        | 获取所有的k               |
| hvals        |                           |
| hgetall      | 获取所有的键值对          |
| hincrby      | key-name k increment      |
| hincrbyfloat |                           |

```bash
127.0.0.1:6379> HMSET k1 k1 v1 k2 v2 k3 v3
OK
127.0.0.1:6379> hmget k1 k1 k2
1) "v1"
2) "v2"
127.0.0.1:6379> HEXISTS k1 a
(integer) 1
127.0.0.1:6379> HEXISTS k1 g
(integer) 0
127.0.0.1:6379> HKEYS k1
1) "k1"
2) "k2"
3) "k3"
4) "a"
5) "c"
127.0.0.1:6379> HVALS k1
1) "v1"
2) "v2"
3) "v3"
4) "b"
5) "d"
127.0.0.1:6379> HGETALL k1
 1) "k1"
 2) "v1"
 3) "k2"
 4) "v2"
 5) "k3"
 6) "v3"
 7) "a"
 8) "b"
 9) "c"
10) "d"
127.0.0.1:6379> HSET k1 12 abc
(integer) 1
127.0.0.1:6379> HINCRBY k1 12 12
(error) ERR hash value is not an integer
127.0.0.1:6379> HSET k1 12 1234  # 这里更改成功了也不会显示 1
(integer) 0
127.0.0.1:6379> HINCRBY k1 12 3
(integer) 1237
127.0.0.1:6379> 
```

### 3.5 有序集合 ###

分值以 IEEE 754 双精度浮点数格式存储

| Command | Desc                                                         |
| ------- | ------------------------------------------------------------ |
| ZADD    | key-name score member [score member]                         |
| ZREM    |                                                              |
| ZCARD   | 成员的数量                                                   |
| ZINCRBY | key-name increment member：add to score                      |
| ZCOUNT  | key-name min max 介于min max 之间的成员数量                  |
| ZRANK   | key-name member 返回成员再集合中排名 (升序排序)              |
| ZSCORE  | key-name member 返回成员的分值                               |
| ZRANGE  | key-name start stop [withscores] 返回排序介于start stop 之间的成员 |

```bash
127.0.0.1:6379> ZADD k1 10 a 11 b 12 c
(integer) 3
127.0.0.1:6379> ZCARD k1
(integer) 3
127.0.0.1:6379> ZINCRBY k1 12 a
"22"
127.0.0.1:6379> ZSCORE k1 b
"11"
127.0.0.1:6379> ZRANK k1 a
(integer) 2
127.0.0.1:6379> ZRANK k1 b
(integer) 0
127.0.0.1:6379> ZRANK k1 c
(integer) 1
127.0.0.1:6379> zadd k1 d 0 
(error) ERR value is not a valid float
127.0.0.1:6379> zadd k1 d 0.1
(error) ERR value is not a valid float
127.0.0.1:6379> zadd k1 0 d
(integer) 1
127.0.0.1:6379> zadd k1 0 hh
(integer) 1
127.0.0.1:6379> ZRANGE k1 0 1
1) "d"
2) "hh"
127.0.0.1:6379> 
```

| Command              | Desc                                                         |
| -------------------- | ------------------------------------------------------------ |
| arevrank             | 返回成员的排名 降序排序                                      |
| zrevrange            | 返回排序介于start stop 之间的成员 [降序]                     |
| zrangebyscore        | key-name min max [withscores] [limit offset count] 返回分值介于min max 之间的成员 |
| zrevrangebyscore     |                                                              |
| zremrangebyrank      | key-name start stop 移除集合中排名介于start stop 之间的所有成员 |
| zremrangebyscore     |                                                              |
| zinterstore 交集运算 | dest-key key-count keys...... [WEIGHTS weight [weight ...]] [AGGREGATE SUM\|MIN\|MAX] |
| zunionstore 并集运算 |                                                              |

```bash
127.0.0.1:6379> ZREVRANK k1 a
(integer) 0
127.0.0.1:6379> ZREVRANK k1 b
(integer) 2
127.0.0.1:6379> ZREVRANK k1 hh
(integer) 3
127.0.0.1:6379> ZREVRANK k1 ddd
(nil)
127.0.0.1:6379> ZREVRANGE k1 2 5
1) "b"
2) "hh"
3) "d"
127.0.0.1:6379> ZREVRANGE k1 2 2
1) "b"
127.0.0.1:6379> ZRANGEBYSCORE k1 0 100 limit 0 2
1) "d"
2) "hh"
127.0.0.1:6379> ZRANGEBYSCORE k1 0 100 limit 1 2
1) "hh"
2) "b"
127.0.0.1:6379> ZREMRANGEBYRANK k1 0 0
(integer) 1
127.0.0.1:6379> 

conn  = redis.Redis(host='192.168.20.123', db=0)
conn.zinterstore('k3', ['k1', 'k2'], aggregate='sum')
conn.zunionstore('k4', ['k1', 'k2'], aggregate='min')
```

### 3.6 发布与订阅 ###

redis中与发布和订阅有关的命令

| 命令         | 描述                                             |
| ------------ | ------------------------------------------------ |
| subscribe    | 订阅一个或者多个频道                             |
| unsubscribe  | 退订一个或者多个频道，不加参数的话退订所有的频道 |
| publish      | channel message                                  |
| psubscribe   | 使用模式匹配的方式订阅频道                       |
| punsubscribe |                                                  |

#### 3.7.1 redis sort ####

```bash
127.0.0.1:6379> rpush k1 23 45 110 4 8 
(integer) 5
127.0.0.1:6379> sort k1
1) "4"
2) "8"
3) "23"
4) "45"
5) "110"
127.0.0.1:6379> sort k1 ALPHA
1) "110"
2) "23"
3) "4"
4) "45"
5) "8"
127.0.0.1:6379> hset k2 f 5
(integer) 1
127.0.0.1:6379> hset k2 f 1
(integer) 0
127.0.0.1:6379> hset k2 f 5
(integer) 0
127.0.0.1:6379> hset k3 f 1
(integer) 1
127.0.0.1:6379> hset k4 f 9
(integer) 1
127.0.0.1:6379> hset k5 f 3
(integer) 1
```

#### 3.7.2 基本的Redis事务 ####

有关的命令： `WATCH` `MULTI` `EXEC` `UNWATCH` `DISCARD`

被 MULTI 和 EXEC 包围的所以指令会一个一个地执行，直到执行完成。当一个事务完成后，redis才会处理其他客户端的命令。

```python
import threading
import time

import redis

conn = redis.Redis('192.168.20.123', db=0)


def trans():
    pp = conn.pipeline()
    pp.incr('trans:')
    time.sleep(0.1)
    pp.incr('trans:', -1)
    print(pp.execute())


if __name__ == '__main__':
    while True:
        for i in range(3):
            threading.Thread(target=trans, ).start()
        time.sleep(0.5)
```

#### 3.7.3 键的过期时间 ####

| 命令      | 描述                       |
| --------- | -------------------------- |
| PERSIST   | 移除键的过期时间           |
| TTL       | 获取键 离过期还有多久      |
| EXPIRE    | 指定键的过期秒数           |
| EXPIREAT  | 指定键的过期时间戳         |
| PTTL      | 获取键 离过期还有多久 毫秒 |
| PEXPIRE   | 指定键的过期毫秒数         |
| PEXPIREAT |                            |

## 4. 数据安全和性能保障 ##

### 4.1 持久化选项 ###

两种持久化方式：

- 快照SAP：把某一时刻的所有数据写入到硬盘中。
- 只追加文件AOF：在执行命令时，将执行的写命令复制到硬盘。

#### 4.1.1 快照持久化 ####

- client 向Redis发送 `BGSAVE` 创建快照。redis调用fork创建一个子进程，子进程负责将快照写入硬盘，父进程继续处理命令请求。
- 发送 `SAVE` 创建快照。使用save后，redis停止接受其他命令，直到快照创建完毕。
- 在conf中，save 60 10000表示在上一次创建快照之后，当 60秒内写入10000次 的条件满足后，redis会调用 `BGSAVE`
- 使用 `SHUTDOWN` , 会执行 SAVE命令。
- 当一个服务器连接另一个服务器时，从服务器向主服务器发送 `SYNC` , 条件满足时，主服务器会执行 `BGSAVE`

#### 4.1.2 AOF 持久化 ####

`appendfsync` 对AOF文件同步的影响

- always
- everysec
- no

client可以发送 `BGREWRITEAOF` 来重写AOF文件。原理相似于 `BGSAVE`

对于两个配置的解释：

- `auto-aof-rewrite-percentage 100` 表示当AOF文件的体积比上一次重写之后的AOF体积大了至少一倍时 redis 执行 `BGREWRITEAOF` 
- `auto-aof-rewrite-min-size 64mb` 

### 4.2 复制 replication ###

#### 4.2.1 配置选项 ####

conf文件：`slaveof host port`

用户可以发送 `SLAVEOF no one` 命令来让服务器终止复制操作。

#### 4.2.2 redis 复制的启动过程 ####

| step | master                                                      | slave                        |
| ---- | ----------------------------------------------------------- | ---------------------------- |
| 1    | waitting                                                    | 连接主服务器，发送SYNC       |
| 2    | 执行 `BGSAVE`, 使用缓冲区记录 `BGSAVE` 之后执行的所有写命令 | 根据配置是否保留现有的数据   |
| 3    | 完成 `BGSAVE` 后，发送给slave                               | 丢弃旧数据，载入master的快照 |
| 4    | 向slave发送缓冲中的写命令                                   | 接受master的写命令           |
| 5    | 向slave同步正常的写命令                                     |                              |

### 4.4 事务 ###

对 `WATCH` 命令的解释：用户使用 watch对键监视之后，直到用户执行 exec 命令的这段时间里，如果有其他客户端抢先对任何被监视的键进行了 替换，更新和删除的操作，当用户尝试执行 exec命令时，事务将失败并返回错误。

`UNWATCH` 可以在 multi 和 watch 命令之间对连接进行重置。

`DISCARD` 可以在 multi 和 exec 之间对连接进行重置。

### 4.5 非事务性流水线 ###

```python
pipe = conn.pipeline(False)
# ......
pipe.execute()
```

## 5. 使用Redis构建支持程序 ##

### 5.1 使用redis来记录日志 ###

#### 5.1.1 最新日志 ####

记录连续更新的最新日志消息 (recent log message)

```python
def log_recent(conn, name, message, severity=logging.INFO, pipe=None):
    severity = str(SEVERITY.get(severity, severity)).lower()
    dest = 'recent:{}:{}'.format(name, severity)
    message = '{} {}'.format(time.asctime(), message)

    pipe = pipe or conn.pipeline()
    pipe.lpush(dest, message)
    pipe.ltrim(dest, 0, 99)
    pipe.execute()
```

#### 5.1.2 常见日志 ####

```python
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
```

### 5.2 计数器和统计数据 ###

time series counter

#### 5.2.1 计数器 ####

**清理旧计数器**

- 任何时候都可能会有新的计数器被添加进来
- 同一时间可能会有多个不同的清理操作在执行
- 对于每天只更新一次的计数器来说，应该控制清理频率
- 如果一个计数器不包含任何数据，那么程序就不应该尝试对它进行清理

### 5.3 查找IP属性城市以及国家 ###

```python
def import_ips_to_redis(conn, filename):
    csv_file = csv.reader(open(filename, 'r'))
    for count, row in enumerate(csv_file):
        start_ip = row[0] if row else ''
        if 'i' in start_ip.lower():
            continue
        if '.' in start_ip:
            start_ip = ip_to_score(start_ip)
        elif start_ip.isdigit():
            start_ip = int(start_ip, 10)
        else:
            continue

        city_id = row[2] + '_' + str(count)
        conn.zadd('ip2cityid:', city_id, start_ip)


def import_cities_to_redis(conn, filename):
    for row in csv.reader(open(filename, 'r', errors='ignore')):
        if len(row) < 4 or not row[0].isdigit():
            continue
        # row = [i.decode('latin-1') for i in row]
        city_id = row[0]
        country = row[1]
        region = row[2]
        city = row[3]
        conn.hset('cityid2city:', city_id, json.dumps([city, region, country]))


def find_city_by_ip(conn, ip_address):
    if isinstance(ip_address, str):
        ip_address = ip_to_score(ip_address)
    city_id = conn.zrevrangebyscore('ip2cityid:', ip_address, 0, start=0, num=1)
    if not city_id:
        return None
    city_id = city_id[0].partition('_')[0]
    return json.loads(conn.hget('cityid2city:', city_id))
```

### 5.4 服务的发现与配置 ###

## 6. Redis 构建Application Components ##

### 6.1 自动补全 ###

#### 6.1.2 通讯录自动补全 ####

名称可以看作abc abca abcd ... abd这样的有序字符串序列，那么查找 abc前缀就是查找 介于 `abbz...` 之后和 `abd` 之前的字符串

比如： `abc{` 位于abd之前，又位于所有带有abc前缀的合法名字之后。

对于unicode来说，可以使用空字节 null 来代替反引号，并使用编码支持的最大值来代替左花括号。

### 6.2 分布式锁 ###

乐观锁与悲观锁

#### 6.2.2 简易锁 ####

- 持有锁的进程因为操作时间过长而导致锁被自动释放，但进程本身并不知晓这一点，可能就会释放其他进程持有的锁
- 一个持有锁并打算执行长时间操作的进程已经崩溃，其他进程不知道但只能等待
- 在一个进程持有的锁过期后，其他多个进程同时尝试去获取锁，而且都获得了锁
- 1和3情况同时出现，多个进程都获得了锁，而且每个进程都以为自己是唯一一个获得锁得进程

```python
def acquire_lock(conn, lock_name, acquire_timeout=10):
    iden = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx('lock:{}'.format(lock_name), iden):
            return iden
        time.sleep(0.001)
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

        except redis.exceptions.WatchError:
            pass

    return False
```

#### 6.2.4 细粒度锁 ####

dogpile效应

#### 6.2.5 带有超时限制特性的锁 ####

```python
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
```

### 6.3 计数信号量 ###

```python
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
```

### 6.4 任务队列 ###

#### 6.4.1 FIFO ####

```python
def send_sold_email_via_queue(conn, seller, item, price, buyer):
    data = {
        'seller_id': seller,
        'item_id': item,
        'price': price,
        'buyer_id': buyer,
        'time': time.time()
    }

    conn.rpush('queue:email', json.dumps(data))


def process_sold_email_queue(conn):
    while not QUIT:
        packed = conn.blpop('queue:email', 30)
        if not packed:
            continue

        to_send = json.loads(packed[1])
        try:
            # fetch_data_and_send_sold_email(to_send)
            pass
        except Exception as err:
            print("Failed to send sold email", err, to_send)
        else:
            print("Sent sold email", to_send)


def worker_watch_queue(conn, queue, callbacks):
    while not QUIT:
        packed = conn.blpop([queue], 30)
        if not packed:
            continue

        name, args = json.loads(packed[1])
        if name not in callbacks:
            print("Unknown callback %s" % name)
            continue
        callbacks[name](*args)
```

**实现优先级队列**
可以使用 `BLPOP` 传入多个队列的key，那么会最先弹出第一个队列的value。

#### 6.4.2 延迟队列 ####

使用有序集合来存储任务，在合适的时机将任务添加到执行队列里面。

```python
def execute_later(conn, queue, name, args, delay=0):
    identifier = str(uuid.uuid4())
    item = json.dumps([identifier, queue, name, args])
    if delay > 0:
        conn.zadd('delayed:', {item: time.time() + delay})
    else:
        conn.rpush('queue:' + queue, item)
    return identifier


def poll_queue(conn):
    while not QUIT:
        item = conn.zrange('delayed:', 0, 0, withscores=True)
        if not item or item[0][1] > time.time():
            time.sleep(.01)
            continue

        item = item[0][0]
        identifier, queue, function, args = json.loads(item)

        locked = acquire_lock(conn, identifier)
        if not locked:
            continue

        if conn.zrem('delayed:', item):
            conn.rpush('queue:' + queue, item)

        release_lock(conn, identifier, locked)
```

### 6.5 消息拉取 ###

#### 6.5.1 单接收者消息的发送与订阅替代品 ####

## 7.搜索 ##

### 7.1 使用redis进行搜索 ###

#### 7.1.1基本搜索原理 ####

需要对文档进行预处理，这个预处理步骤被称为 indexing，而我们要创建的结构被称为反向索引 inverted indexes .

这个过程就是从文档 --> 到redis集合的过程。

![image-20210405221703197](.assets/image-20210405221703197.png)

**1. 基本索引操作**

从文档里提取单词的过程通常被称为语法分析 parsing 和 标记化 tokenization 。这个过程可以产生出一系列用于标识文档的标记 token ，有时也被称为单词 word。

标记化的一个常见的附加步骤，就是移除内容中的非用词 stop word。

```python
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
    words = set()
    for match in WORDS_RE.finditer(content.lower()):
        word = match.group().strip("'")
        if len(word) >= 2:
            words.add(word)
    return words - STOP_WORDS


def index_document(conn, doc_id, content):
    words = tokenize(content)

    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd('idx:' + word, doc_id)
    return len(pipeline.execute())
```

**2. 基本搜索操作**

把给定单词集合里面的所有文档id都找出来，然后从中找到那些在所有单词集合里面都出现了的文档。

1. 使用交集处理反向索引
2. 使用并集处理相同意思单词进行搜索。

```python
def _set_common(conn, method, names, ttl=30, execute=True):
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
```

**3.分析并执行搜索**

将文本查询语句转换成搜索操作的函数。

- `-` 表示用户不希望包含这个单词的文档出现在搜索结构里面，使用 difference
- `+` 表示同义词。使用 union
- 都没有表示 交集

```python
QUERY_RE = re.compile("[+-]?[a-z']{2,}")


def parse(query):
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
```

#### 7.1.2 对搜索结果进行排序 ####

关联度计算问题

```python
def search_and_sort(conn, query, tid=None, ttl=300, sort="-updated", start=0, num=20):
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
```

### 7.2 有序索引 ###

#### 7.2.1 使用有序集合对搜索结果进行排序 ####

使用有序集合(zset)保存文章(document) 的更新时间以及文章获得的投票数量。

```python
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
```

### 7.3 广告定向 ###

使用集合和有序集合实现广告定向引擎 ad-targeting engine

#### 7.3.1 广告服务器 ####

广告服务器会接收分钟各样的信息，根据信息找出能够通过点击，浏览或者动作获得最大经济收益的广告。

需要一系列定向的参数：比如IP，操作系统，web浏览器，浏览的内容....

#### 7.3.2 对广告进行索引 ####

按展示次数计费：CPM (cost per mille)

按点击次数计费：CPC

按执行次数计费：CPA

不同的类型之间应该换算成同一种单位：eCPM

**计算CPC广告的eCPM** ：广告的每次点击价格乘以广告的点击通过率 (`click-through rate`) 乘以1000 得出的结果就是 eCPM。

**计算CPA广告的eCPM** ：广告的点击通过率，用户在广告投放者的目标页面上执行动作的概率，被执行动作的价格相乘。

## 8. 构建社交应用 ##

### 8.1 用户和状态 ###

#### 8.1.1 用户信息 ####

![image-20210407200243624](.assets/image-20210407200243624.png)

```python
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
        'id': id,
        'name': name,
        'followers': 0,
        'following': 0,
        'posts': 0,
        'signup': time.time(),
    })
    pipe.execute()
    release_lock(conn, 'user:' + lower_login, lock)
    return uid
```

#### 8.1.2 状态消息 ####

![image-20210407201701729](.assets/image-20210407201701729.png)

```python
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
```

### 8.2 主页时间线 ###

![image-20210407202556985](.assets/image-20210407202556985.png)

```python
def get_status_messages(conn, uid, timeline='home:', page=1, count=30):
    status_list = conn.zrevrange('{}{}'.format(timeline, uid), (page - 1) * count, page * count - 1)
    pipe = conn.pipeline(True)
    for sid in status_list:
        pipe.hgetall('status:{}'.format(sid))

    return [_f for _f in pipe.execute() if _f]
```

### 8.5 流API ###

流API的相关问题：

- 流API需要对外公开哪些事件?
- 是否需要进行访问限制
- 流API应该提供哪些过滤选项

## 11. Lua脚本编程 ##

#### 11.1.1 Lua脚本载入到Redis ####

`SCRIPT LOAD` 

`EVALSHA`

| Lua值                  | Python值                                |
| ---------------------- | --------------------------------------- |
| true                   | 1                                       |
| false                  | None                                    |
| nil                    | 他会让脚本停止返回Lua表格中剩余的任何值 |
| float                  | 转换成整数                              |
| 巨大的浮点数           | 转换成当前python版本的最小整数          |
| strings                | str                                     |
| 1 到 +-2^53 - 1 的整数 | 无需转换                                |

一次的lua脚本执行，可以看作一次原子的操作

已经对结构进行了修改的lua脚本将无法被中断

