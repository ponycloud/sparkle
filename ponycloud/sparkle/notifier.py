#!/usr/bin/python -tt

from autobahn.wamp import WampServerFactory, WampCraServerProtocol, exportRpc
from autobahn.websocket import listenWS
from ponycloud.common.auth import verify_token
from simplejson import loads

__all__ = ['Notifier']

class Notifier(WampServerFactory):
    # Used for model lazy loading
    def load(self, model):
        self.model = model
        self.protocol = NotificationsProtocol
        self.topic_uri = '[ponycloud:notifications]'

    # Call this method to send something to certain tenant
    def publish(self, tenant_uuid, event):
        self.dispatch('{}/{}'.format(self.topic_uri, tenant_uuid), event)

    def start(self):
        def make_model_handler(operation):
            def model_handler(table, row):
                allowed = row.get_allowed(table, self.model)
                to_publish = {'operation': operation,
                            'type': table.name,
                            'pkey-name': table.pkey,
                            'pkey': row.pkey,
                            'desired': row.desired,
                            'current': row.current}
                # Publish event to all interested tenants
                for channel in allowed:
                    self.publish(channel, to_publish)
                # ...and report to alicorns
                self.publish('alicorns', to_publish)
            return model_handler

        for item in self.model:
            self.model[item].on_create_state(make_model_handler('create'))
            self.model[item].on_update_state(make_model_handler('update'))
            self.model[item].on_before_delete_state(make_model_handler('delete'))
        listenWS(self)

    # Function for determining permissions for given username
    # TODO Now it's just a list of tenants the user is member of
    def get_permissions(self, username):
        tenants = []
        # Determine if user is alicorn or not
        alicorn = self.model['user'][username].desired['alicorn']
        # Get users membership
        rows = self.model['member'].list(user=username)
        if rows is None:
            return None
        for row in rows:
            tenants.append(row.desired['tenant'])

        else:
            pubsub = []
            for tenant in tenants:
                pubsub.append({'uri': '{}/{}'.format(self.topic_uri,tenant),
                                       'prefix': True,
                                       'pub': False,
                                       'sub': True})
            if alicorn:
                pubsub.append({'uri': '{}/{}'.format(self.topic_uri,'alicorns'),
                                       'prefix': True,
                                       'pub': False,
                                       'sub': True})

            return { username: {'pubsub': pubsub} }

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

    def getAuthPermissions(self, username):
      ## return permissions which will be granted for the auth key
      ## when the authentication succeeds
      permissions = self.factory.get_permissions(username)
      if permissions is None:
          return None
      else:
          return {'permissions': permissions}

    def auth(self, token):
        if token is None:
            return None

        token = loads(token.decode('base64'))

        if verify_token(token, self.factory.passkey):
            # TODO Many things could go wrong here
            username = token[0].split(':')[1]
            self._clientAuthenticated = True
            self.onAuthenticated(username)
            return self.getAuthPermissions(username)

    def onAuthenticated(self, authKey):
      ## register PubSub topics from the auth permissions
      perms = self.getAuthPermissions(authKey)
      self.registerForPubSubFromPermissions(perms['permissions'][authKey])

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
