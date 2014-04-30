import time
from datetime import datetime

from storm.locals import *
from warp import runtime
from warp.common import access
from warp.common.store import createStore

from warp.common.schema import stormSchema

@stormSchema.versioned
class Avatar(Storm):
    __version__ = "warp_1"
    __storm_table__ = "warp_avatar"

    id = Int(primary=True)
    email = Unicode()
    password = Unicode()

    _roles = None
    def _getRoles(self):
        if self._roles is None:
            roleLookup = runtime.config['roles']
            avatar_roles = runtime.avatar_store.find(
                AvatarRole,
                AvatarRole.avatar == self
            ).order_by(AvatarRole.position)
            self._roles = tuple(
                [roleLookup[ar.role_name]
                    for ar in avatar_roles if ar.role_name in roleLookup] +
                [roleLookup[r] for r in
                    runtime.config['defaultRoles']]
            )

        return self._roles
    roles = property(_getRoles)


    def __repr__(self):
        return "<Avatar '%s'>" % self.email.encode("utf-8")


_MESSAGES = {}

def nowstamp():
    return int(time.mktime(datetime.utcnow().timetuple()))


class SessionManager(object):
    """
    Default DB-backed session handling
    """

    counter = 0

    def createSession(self):
        uid = self._mkuid()
        session = DBSession()
        session.uid = uid
        runtime.avatar_store.add(session)
        runtime.avatar_store.commit()
        return session

    def getSession(self, uid):
        return runtime.avatar_store.get(DBSession, uid)

    def _mkuid(self):
        from twisted.python.hashlib import md5
        import random
        self.counter = self.counter + 1
        return md5("%s_%s" % (str(random.random()) , str(self.counter))).hexdigest()


@stormSchema.versioned
class DBSession(Storm):
    # FIXME HXP: This breaks integration with MySQL and SQLite, but those are
    # not working anyway due to the missing touched column.
    __version__ = "hxp_2"
    __storm_table__ = "warp_session"

    uid = RawStr(primary=True)
    avatar_id = Int()
    avatar = Reference(avatar_id, Avatar.id)
    touched = Int(default_factory=nowstamp)

    isPersistent = Bool(default=False)

    language = u"en_US"
    messages = None
    afterLogin = None

    _touch_granularity = 10

    def __storm_loaded__(self):
        if self.language is None:
            self.language = u"en_US"
        if self.touched is None:
            self.touched = nowstamp()
            runtime.avatar_store.commit()


    def addFlashMessage(self, msg, in_memory=False, *args, **kwargs):

        # TODO HXP: remove this after DB messages have been tested
        if in_memory:
            if self.uid not in _MESSAGES:
                _MESSAGES[self.uid] = []
            _MESSAGES[self.uid].append((msg, args, kwargs))
            return

        store = createStore()
        message_storage = store.find(DBSessionStorage, And(
            DBSessionStorage.uid==self.uid,
            DBSessionStorage.key==u'messages',
        )).one()
        if message_storage:
            message_storage.value.append((msg, args, kwargs))
        else:
            message_storage = DBSessionStorage()
            message_storage.uid = self.uid
            message_storage.key = u'messages'
            message_storage.value = [(msg, args, kwargs)]
            store.add(message_storage)
        store.commit()
        store.close()


    def getFlashMessages(self, clear=True, in_memory=False):

        # TODO HXP: remove this after DB messages have been tested
        if in_memory:
            if self.uid not in _MESSAGES:
                return []
            messages = _MESSAGES[self.uid][:]
            if clear:
                del _MESSAGES[self.uid]
            return messages

        store = createStore()
        message_storage = store.find(DBSessionStorage, And(
            DBSessionStorage.uid==self.uid,
            DBSessionStorage.key==u'messages',
        )).one()
        if not message_storage:
            return []
        messages = message_storage.value
        if clear:
            store.remove(message_storage)
            store.commit()
        store.close()
        return messages


    def hasAvatar(self):
        return self.avatar_id is not None

    def setPersistent(self, isPersistent):
        self.isPersistent = isPersistent
        runtime.avatar_store.commit()

    def setAvatarID(self, avatarID):
        self.avatar_id = avatarID
        runtime.avatar_store.commit()

    def age(self):
        return nowstamp() - self.touched

    def touch(self):
        if self.age() > self._touch_granularity:
            self.touched = nowstamp()
            runtime.avatar_store.commit()

    def __repr__(self):
        return "<Session '%s'>" % self.uid


@stormSchema.versioned
class DBSessionStorage(Storm):
    __version__ = 'hxp_1'
    __storm_table__ = 'warp_session_storage'
    __storm_primary__ = 'uid', 'key'
    uid = RawStr()
    key = Unicode()
    value = JSON()


# ---------------------------


@stormSchema.versioned
class AvatarRole(Storm):
    __version__ = "warp_1"
    __storm_table__ = "warp_avatar_role"

    id = Int(primary=True)

    avatar_id = Int()
    avatar = Reference(avatar_id, "Avatar.id")

    role_name = RawStr()

    position = Int()
