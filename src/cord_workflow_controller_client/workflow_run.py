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
Workflow Run

This module implements Workflow Run interface
"""

import json
import socketio

from .countdown_latch import CountDownLatch
from .utils import get_noop_logger, gen_id, gen_seq_id
from .errors import ClientRPCError, ClientInputError, ClientResponseError

WAIT_TIMEOUT = 10  # 10 seconds

GREETING = 'cord.workflow.ctlsvc.greeting'
WORKFLOW_RUN_COUNT_EVENTS = 'cord.workflow.ctlsvc.workflow.run.count'
WORKFLOW_RUN_FETCH_EVENT = 'cord.workflow.ctlsvc.workflow.run.fetch'
WORKFLOW_RUN_NOTIFY_EVENT = 'cord.workflow.ctlsvc.workflow.run.notify'


class WorkflowRun(object):
    def __init__(self, workflow_id, workflow_run_id, logger=None, name=None):
        self.sio = socketio.Client()
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id

        if logger:
            self.logger = logger
        else:
            self.logger = get_noop_logger()

        if name:
            self.name = name
        else:
            self.name = 'workflow_run_%s' % gen_id()

        self.req_id = gen_seq_id()

        # set sio handlers
        self.logger.debug('Setting event handlers to Socket.IO')
        self.sio.on('connect', self.__on_sio_connect)
        self.sio.on('disconnect', self.__on_sio_disconnect)
        self.sio.on(GREETING, self.__on_greeting_message)
        self.sio.on(WORKFLOW_RUN_COUNT_EVENTS, self.__on_count_events_message)
        self.sio.on(WORKFLOW_RUN_FETCH_EVENT, self.__on_fetch_event_message)
        self.sio.on(WORKFLOW_RUN_NOTIFY_EVENT, self.__on_notify_event_message)

        self.handlers = {
            'connect': self.__noop_connect_handler,
            'disconnect': self.__noop_disconnect_handler,
            'notify': self.__noop_notify_handler
        }

        # key is req_id
        self.pending_requests = {}

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

    def __noop_notify_handler(self, workflow_id, workflow_run_id, topic):
        self.logger.debug('no-op notify handler')

    def __get_next_req_id(self):
        req_id = self.req_id
        self.req_id += 1
        return req_id

    def __on_greeting_message(self, data):
        self.logger.debug('received a greeting message from the server')

    def __on_notify_event_message(self, data):
        """
        Handler for a notify event
        REQ = {
            'topic': <topic>
        }
        """
        self.logger.info('received a notify event message from the server')
        topic = data['topic']

        self.logger.info('a notify event message - topic (%s)' % topic)
        if topic:
            handler = self.handlers['notify']
            if callable(handler):
                self.logger.info('calling a notify event handler - %s' % handler)
                handler(self.workflow_id, self.workflow_run_id, topic)

    def __on_count_events_message(self, data):
        self.__on_response(WORKFLOW_RUN_COUNT_EVENTS, data)

    def __on_fetch_event_message(self, data):
        self.__on_response(WORKFLOW_RUN_FETCH_EVENT, data)

    def __check_pending_request(self, req_id):
        """
        Check a pending request
        """
        if req_id in self.pending_requests:
            return True
        return False

    def __put_pending_request(self, api, params):
        """
        Put a pending request to a queue
        """
        req_id = self.__get_next_req_id()
        latch = CountDownLatch()
        params['req_id'] = req_id  # inject req_id
        self.sio.emit(api, params)
        self.pending_requests[req_id] = {
            'req_id': req_id,
            'latch': latch,
            'api': api,
            'params': params,
            'result': None
        }
        return req_id

    def __wait_response(self, req_id):
        """
        Wait for completion of a request
        """
        if req_id in self.pending_requests:
            req = self.pending_requests[req_id]
            # python v 3.2 or below does not return a result
            # that tells whether it is timedout or not
            return req['latch'].wait(WAIT_TIMEOUT)
        else:
            self.logger.error(
                'cannot find a pending request (%s) from a queue' % req_id
            )
            raise ClientRPCError(
                req_id,
                'cannot find a pending request (%s) from a queue' % req_id
            )

    def __complete_request(self, req_id, result):
        """
        Compelete a pending request
        """
        if req_id in self.pending_requests:
            req = self.pending_requests[req_id]
            req['latch'].count_down()
            req['result'] = result
            return

        self.logger.error(
            'cannot find a pending request (%s) from a queue' % req_id
        )
        raise ClientRPCError(
            req_id,
            'cannot find a pending request (%s) from a queue' % req_id
        )

    def __pop_pending_request(self, req_name):
        """
        Pop a pending request from a queue
        """
        return self.pending_requests.pop(req_name, None)

    def connect(self, url):
        """
        Connect to the given url
        """
        query_string = 'id=%s&type=workflow_run&name=%s&workflow_id=%s&workflow_run_id=%s' % \
            (self.name, self.name, self.workflow_id, self.workflow_run_id)
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

    def wait(self):
        self.sio.wait()

    def sleep(self, sec):
        self.sio.sleep(sec)

    def get_handlers(self):
        return self.handlers

    def set_handlers(self, new_handlers):
        for k in self.handlers:
            if k in new_handlers:
                self.handlers[k] = new_handlers[k]

    def __request(self, api, params={}):
        if api and params:
            req_id = self.__put_pending_request(api, params)
            self.logger.debug('waiting for a response for req_id (%s)' % req_id)
            self.__wait_response(req_id)  # wait for completion
            req = self.__pop_pending_request(req_id)
            if req:
                if req['latch'].get_count() > 0:
                    # timed out
                    self.logger.error('request (%s) timed out' % req_id)
                    raise ClientRPCError(
                        req_id,
                        'request (%s) timed out' % req_id
                    )
                else:
                    return req['result']
            else:
                self.logger.error('cannot find a pending request (%s) from a queue' % req_id)
                raise ClientRPCError(
                    req_id,
                    'cannot find a pending request (%s) from a queue' % req_id
                )
        else:
            self.logger.error(
                'invalid arguments api (%s), params (%s)' %
                (api, json.dumps(params))
            )
            raise ClientInputError(
                'invalid arguments api (%s), params (%s)' %
                (api, json.dumps(params))
            )

    def __on_response(self, api, result):
        if result and 'req_id' in result:
            self.logger.debug('completing a request (%s)' % result['req_id'])
            self.__complete_request(result['req_id'], result)
        else:
            self.logger.error(
                'invalid arguments api (%s), result (%s)' %
                (api, json.dumps(result))
            )
            raise ClientInputError(
                'invalid arguments api (%s), result (%s)' %
                (api, json.dumps(result))
            )

    def count_events(self):
        """
        Count events.
        """
        result = self.__request(WORKFLOW_RUN_COUNT_EVENTS, {
            'workflow_id': self.workflow_id,
            'workflow_run_id': self.workflow_run_id
        })
        if result['error']:
            self.logger.error(
                'request (%s) failed with an error - %s' %
                (result['req_id'], result['message'])
            )
            raise ClientResponseError(
                'request (%s) failed with an error - %s' %
                (result['req_id'], result['message'])
            )
        else:
            return result['result']

    def fetch_event(self, task_id, topic):
        """
        Fetch an event.
        """
        if task_id and topic:
            result = self.__request(WORKFLOW_RUN_FETCH_EVENT, {
                'workflow_id': self.workflow_id,
                'workflow_run_id': self.workflow_run_id,
                'task_id': task_id,
                'topic': topic
            })
            if result['error']:
                self.logger.error(
                    'request (%s) failed with an error - %s' %
                    (result['req_id'], result['message'])
                )
                raise ClientResponseError(
                    'request (%s) failed with an error - %s' %
                    (result['req_id'], result['message'])
                )
            else:
                return result['result']
        else:
            self.logger.error(
                'invalid arguments task_id (%s), topic (%s)' %
                (task_id, topic)
            )
            raise ClientInputError(
                'invalid arguments task_id (%s), topic (%s)' %
                (task_id, topic)
            )
