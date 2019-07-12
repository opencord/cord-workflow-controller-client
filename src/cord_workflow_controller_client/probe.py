#!/usr/bin/env python3

# Copyright 2019-present Open Networking Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Workflow Probe

This module implements Workflow Probe interface
"""

import json
import socketio

from .utils import get_noop_logger, gen_id
from .errors import ClientInputError

GREETING = 'cord.workflow.ctlsvc.greeting'


class Probe(object):
    def __init__(self, logger=None, name=None):
        self.sio = socketio.Client()

        if logger:
            self.logger = logger
        else:
            self.logger = get_noop_logger()

        if name:
            self.name = name
        else:
            self.name = 'probe_%s' % gen_id()

        # set sio handlers
        self.logger.debug('Setting event handlers to Socket.IO')
        self.sio.on('connect', self.__on_sio_connect)
        self.sio.on('disconnect', self.__on_sio_disconnect)
        self.sio.on(GREETING, self.__on_greeting_message)

        self.handlers = {
            'connect': self.__noop_connect_handler,
            'disconnect': self.__noop_disconnect_handler
        }

    def set_logger(self, logger):
        self.logger = logger

    def get_logger(self):
        return self.logger

    def __on_sio_connect(self):
        self.logger.debug('connected to the server')
        handler = self.handlers['connect']
        if callable(handler):
            handler()

    def __noop_connect_handler(self):
        self.logger.debug('no-op connect handler')

    def __on_sio_disconnect(self):
        self.logger.debug('disconnected from the server')
        handler = self.handlers['disconnect']
        if callable(handler):
            handler()

    def __noop_disconnect_handler(self):
        self.logger.debug('no-op disconnect handler')

    def __on_greeting_message(self, data):
        self.logger.debug('received a greeting message from the server')

    def connect(self, url):
        """
        Connect to the given url
        """
        query_string = 'id=%s&type=probe&name=%s' % (self.name, self.name)
        connect_url = '%s?%s' % (url, query_string)

        if not (connect_url.startswith('http://') or connect_url.startswith('https://')):
            connect_url = 'http://%s' % connect_url

        self.logger.debug('Connecting to a Socket.IO server (%s)' % connect_url)
        self.sio.connect(url=connect_url, transports=['websocket'])

    def disconnect(self):
        """
        Disconnect from the server
        """
        self.sio.disconnect()

    def get_handlers(self):
        return self.handlers

    def set_handlers(self, new_handlers):
        for k in self.handlers:
            if k in new_handlers:
                self.handlers[k] = new_handlers[k]

    def emit_event(self, event, body):
        """
        Emit event to Workflow Controller
        """
        if event and body:
            self.sio.emit(event, body)
        else:
            self.logger.error(
                'invalid arguments event(%s), body(%s)' %
                (event, json.dumps(body))
            )
            raise ClientInputError(
                'invalid arguments event(%s), body(%s)' %
                (event, json.dumps(body))
            )
