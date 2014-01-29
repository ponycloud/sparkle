#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Notifier']

from simplejson import loads
from twisted.internet import reactor
from autobahn.wamp import WampServerFactory, WampCraServerProtocol

from sparkle.schema import schema
from sparkle.auth import extract_token


class Notifier(WampServerFactory):
    def set_model(self, model):
        """
        Hook notifier to specified model.
        """

        self.model = model
        self.protocol = NotificationsProtocol
        self.topic_uri = '[ponycloud:notifications]'

    def publish(self, channel, event):
        """Publish notification on a specified channel."""
        self.dispatch(self.topic_uri + '/' + channel, event)

    def start(self):
        """
        Create hooks in model used for the distribution of notifications.
        """

        def update_handler(old, new):
            message = {
                'type':      new.table.name,
                'pkey_name': new.table.schema.pkey,
                'pkey':      new.pkey,
                'desired':   new.desired,
                'current':   new.current,
            }

            allowed = old.get_tenants().union(new.get_tenants())

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

        self.model.add_callback(update_handler)
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
