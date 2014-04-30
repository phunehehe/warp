from twisted.python import log

from storm.locals import *
from storm.uri import URI
from storm.exceptions import DatabaseError

from warp.runtime import avatar_store, config, sql


# TODO HXP: maybe add connection pooling here?
class ManagedStore():
    def __enter__(self):
        self.store = Store(create_database(config['db']))
        return self.store
    def __exit__(self, type, value, traceback):
        self.store.close()


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
