#!/usr/bin/python -tt
from autobahn.wamp import WampServerFactory, \
                          WampCraServerProtocol, \
                          exportRpc
from autobahn.websocket import listenWS
from ponycloud.common.auth import verify_token

__all__ = ['Notifier']
class Notifier(WampServerFactory):

    def load(self, model):
        self.model = model
        self.protocol = NotificationsProtocol
        self.topic_uri = '[ponycloud:notifications]'

    def publish(self, tenant_uuid, event):
        self.dispatch('{}/{}'.format(self.topic_uri, tenant_uuid), event)

    def _model_handler(self, table, row):
        #TODO Need to know to whom we need to talk
        self.publish('76764219-6bd4-4278-8b7b-659fc43c939e', 
                {'type': table.name, 'desired': row.desired, 'current': row.current})

    def start(self):
        for item in self.model:
            self.model[item].on_after_row_update(self._model_handler)
        listenWS(self)

    def get_permissions(self, tenant_uuid):

        tenant = self.model['tenant'][tenant_uuid]
        if tenant is None:
            return None
        else:
            pubsub = {'uri': '{}/{}'.format(self.topic_uri,tenant.desired['uuid']),
                                   'prefix': True,
                                   'pub': False,
                                   'sub': True}
            return { tenant.desired['uuid']: {'pubsub': [pubsub]} }

    def get_secret(self, tenant):
        return 'secret'


class NotificationsProtocol(WampCraServerProtocol):
    """
    Authenticating WAMP server using WAMP-Challenge-Response-Authentication ("WAMP-CRA").
    """

    def onSessionOpen(self):
      self.clientAuthTimeout = 0
      self.clientAuthAllowAnonymous = False
      WampCraServerProtocol.onSessionOpen(self)
      self.registerMethodForRpc("http://api.wamp.ws/procedure#auth",
                               self,
                               NotificationsProtocol.auth)

    def getAuthPermissions(self, authKey):
      ## return permissions which will be granted for the auth key
      ## when the authentication succeeds
      permissions = self.factory.get_permissions(authKey)
      if permissions is None:
          return None
      else:
          return {'permissions': permissions}

    def getAuthSecret(self, authKey):
      return self.factory.get_secret(authKey)

    def auth(self, token):
        if token is None:
            return None
        if verify_token(token):
            self._clientAuthenticated = True
            self.onAuthenticated(token[0])
            return self.getAuthPermissions(token[0])

    def onAuthenticated(self, authKey):
      ## register PubSub topics from the auth permissions
      perms = self.getAuthPermissions(authKey)
      self.registerForPubSubFromPermissions(perms['permissions'][authKey])

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
