from twisted.python import log
from Queue import Queue, Empty

from storm.locals import *
from storm.uri import URI
from storm.exceptions import DatabaseError

from warp.runtime import avatar_store, config, sql


class ManagedStore():

    def __init__(self, store, pool):
        self.store = store
        self.pool = pool

    def __enter__(self):
        return self.store

    def __exit__(self, type, value, traceback):
        # Whatever needs saving should have already been committed
        self.store.rollback()
        self.pool.storeDone(self.store)


class DBConnectionPool():

    # TODO HXP: parameterize this
    POOL_LIMIT = 10

    connections = Queue()
    in_pool = 0

    # HXP: There are 3 ways to get a connection using Queue:
    #  1. Use get_nowait(), which raises Empty if the pool is empty. This is
    #     suitable for most cases. Client code should handle Empty, or
    #     POOL_LIMIT should be raised.
    #  2. Use get(), which blocks until there is something in the pool. This
    #     may be more suitable if the store is going to be kept for a long
    #     running task in separate threads.
    #  3. Use get() with a timeout. This may be useful in some cases, though I
    #     can't think of any right now.
    # Also note that this is not thread-safe, so it should only be used in the
    # main thread.
    @classmethod
    def getConnection(klass):
        try:
            store = klass.connections.get_nowait()
        except Empty:
            # XXX HXP: This doesn't look like the best place to populate the
            # pool
            if klass.in_pool < klass.POOL_LIMIT:
                store = Store(create_database(config['db']))
                klass.in_pool += 1
            else:
                raise
        return ManagedStore(store, klass)

    @classmethod
    def storeDone(klass, store):
        klass.connections.put(store)
        # XXX HXP: maybe close each store after n uses?


def setupStore():
    avatar_store.__init__(create_database(config['db']))

    if config.get('trace'):
        import sys
        from storm.tracer import debug
        debug(True, stream=sys.stdout)

    # Only sqlite uses this now
    sqlBundle = getCreationSQL(avatar_store)
    if not sqlBundle:
        return

    tableExists = sql['tableExists'] = sqlBundle['tableExists']

    for (table, creationSQL) in sqlBundle['creations']:

        if not tableExists(avatar_store, table):

            # Unlike log.message, this works during startup
            print "~~~ Creating Warp table '%s'" % table

            if not isinstance(creationSQL, tuple): creationSQL = [creationSQL]
            for sqlCmd in creationSQL: avatar_store.execute(sqlCmd)
            avatar_store.commit()


def getCreationSQL(store):
    connType = store._connection.__class__.__name__
    return {
        'SQLiteConnection': {
            'tableExists': lambda s, t: bool(s.execute("SELECT count(*) FROM sqlite_master where name = '%s'" % t).get_one()[0]),
            'creations': [
                ('warp_avatar', """
                CREATE TABLE warp_avatar (
                    id INTEGER NOT NULL PRIMARY KEY,
                    email VARCHAR,
                    password VARCHAR,
                    UNIQUE(email))"""),
                ('warp_session', """
                CREATE TABLE warp_session (
                    uid BYTEA NOT NULL PRIMARY KEY,
                    avatar_id INTEGER REFERENCES warp_avatar(id) ON DELETE CASCADE)"""),
                ('warp_avatar_role', """
                CREATE TABLE warp_avatar_role (
                    id INTEGER NOT NULL PRIMARY KEY,
                    avatar_id INTEGER NOT NULL REFERENCES warp_avatar(id) ON DELETE CASCADE,
                    role_name BYTEA NOT NULL,
                    position INTEGER NOT NULL DEFAULT 0)"""),
                ],
            },
        'MySQLConnection': {
            'tableExists': lambda s, t: bool(s.execute("""
                   SELECT count(*) FROM information_schema.tables
                   WHERE table_schema = ? AND table_name=?""",
               (URI(config['db']).database, t)).get_one()[0]),
            'creations': [
                ('warp_avatar', """
                CREATE TABLE warp_avatar (
                    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(64),
                    password VARCHAR(32),
                    UNIQUE(email)
                  ) engine=InnoDB, charset=utf8"""),
                ('warp_session', """
                CREATE TABLE warp_session (
                    uid VARBINARY(32) NOT NULL PRIMARY KEY,
                    avatar_id INTEGER REFERENCES warp_avatar(id) ON DELETE CASCADE
                  ) engine=InnoDB, charset=utf8"""),
                ('warp_avatar_role', """
                CREATE TABLE warp_avatar_role (
                    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    avatar_id INTEGER NOT NULL REFERENCES warp_avatar(id) ON DELETE CASCADE,
                    role_name VARBINARY(32) NOT NULL,
                    position INTEGER NOT NULL
                  ) engine=InnoDB, charset=utf8"""),
                ],
            },
    }.get(connType)
