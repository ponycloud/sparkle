#!/usr/bin/python -tt
from autobahn.wamp import WampServerFactory, \
    WampCraServerProtocol

from twisted.internet import reactor
from auth import extract_token
from simplejson import loads
from ponycloud.common import schema

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
        """
        Main method called on start of the notifier. This method creates hooks in model used
        for the distribution of notifications.

        """
        def make_model_handler(operation):
            def model_handler(table, row):
                message = {
                    'operation': operation,
                    'type': table.name,
                    'pkey-name': table.schema.pkey,
                    'pkey': row.pkey,
                    'desired': row.desired,
                    'current': row.current
                }

                allowed = row.get_tenants()

                # TODO: Send info about entities that would match
                #       shared-access endpoints, such as public images.

                if len(allowed) > 0:
                    # Publish event to all interested tenants.
                    for tenant in allowed:
                        self.publish(tenant, message)
                else:
                    # Publish event only to alicorns.
                    # NOTE: Due to the missing public entity notifications
                    #       alicorns now get notifications about them as well.
                    self.publish('alicorns', message)

            return model_handler

        for table_name, table in self.model.iteritems():
            table.on_create_state(make_model_handler('create'))
            table.on_update_state(make_model_handler('update'))
            table.on_before_delete_state(make_model_handler('delete'))

        reactor.listenTCP(self.port, self)

    def get_channels(self, tenants=[], public=False, alicorn=False):
        """
        Create pubsub channels definition that are used for publishing notifications
        """

        def get_channel(name):
            """
            Generate template for each channel
            """
            return {'uri': '{}/{}'.format(self.topic_uri, name),
                    'prefix': True,
                    'pub': False,
                    'sub': True}

        pubsub = []
        # Public notifications
        if public:
            pubsub.append(get_channel('public'))
        # Tenant specific
        for tenant in tenants:
            pubsub.append(get_channel(tenant))
        # Alicorns only
        if alicorn:
            pubsub.append(get_channel('alicorns'))

        return {'pubsub': pubsub}

    # Function for determining permissions for given token
    def get_permissions(self, token):
        if 'tenant' in token:
            return self.get_channels(tenants=[token['tenant']], public=True)
        else:
            username = token['user']
            # Determine if user is alicorn or not
            # TODO user might be already gone
            alicorn = self.model['user'][username].desired['alicorn']
            pub_sub = self.get_channels(alicorn=alicorn)
            return pub_sub


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

    def auth(self, token):
        """
        Called from client to give him permissions
        to pubsub channels.
        """
        try:
            token = extract_token(token, self.factory.apikey)
            token = loads(token)
        except ValueError:
            return False

        self._clientAuthenticated = True
        permissions = self.factory.get_permissions(token)
        self.registerForPubSubFromPermissions(permissions)
        return permissions

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
