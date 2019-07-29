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
Workflow Manager

This module implements Workflow Manager interface
"""

import json
import socketio

from .countdown_latch import CountDownLatch
from .utils import get_noop_logger, gen_id, gen_seq_id
from .errors import ClientRPCError, ClientInputError, ClientResponseError

WAIT_TIMEOUT = 10  # 10 seconds

# controller -> manager
GREETING = 'cord.workflow.ctlsvc.greeting'
WORKFLOW_KICKSTART = 'cord.workflow.ctlsvc.workflow.kickstart'
WORKFLOW_CHECK_STATUS = 'cord.workflow.ctlsvc.workflow.check.status'
WORKFLOW_CHECK_STATUS_BULK = 'cord.workflow.ctlsvc.workflow.check.status_bulk'

# manager -> controller -> manager
WORKFLOW_REGISTER = 'cord.workflow.ctlsvc.workflow.register'
WORKFLOW_REGISTER_ESSENCE = 'cord.workflow.ctlsvc.workflow.register_essence'
WORKFLOW_LIST = 'cord.workflow.ctlsvc.workflow.list'
WORKFLOW_LIST_RUN = 'cord.workflow.ctlsvc.workflow.run.list'
WORKFLOW_CHECK = 'cord.workflow.ctlsvc.workflow.check'
WORKFLOW_REMOVE = 'cord.workflow.ctlsvc.workflow.remove'
WORKFLOW_REMOVE_RUN = 'cord.workflow.ctlsvc.workflow.run.remove'
WORKFLOW_REPORT_NEW_RUN = 'cord.workflow.ctlsvc.workflow.report_new_run'
WORKFLOW_REPORT_RUN_STATUS = 'cord.workflow.ctlsvc.workflow.report_run_status'
WORKFLOW_REPORT_RUN_STATUS_BULK = 'cord.workflow.ctlsvc.workflow.report_run_status_bulk'


class Manager(object):
    def __init__(self, logger=None, name=None):
        self.sio = socketio.Client()

        if logger:
            self.logger = logger
        else:
            self.logger = get_noop_logger()

        if name:
            self.name = name
        else:
            self.name = 'manager_%s' % gen_id()

        self.req_id = gen_seq_id()

        # set sio handlers
        self.logger.debug('Setting event handlers to Socket.IO')
        self.sio.on('connect', self.__on_sio_connect)
        self.sio.on('disconnect', self.__on_sio_disconnect)
        self.sio.on(WORKFLOW_KICKSTART, self.__on_kickstart_message)
        self.sio.on(WORKFLOW_CHECK_STATUS, self.__on_check_status_message)
        self.sio.on(WORKFLOW_CHECK_STATUS_BULK, self.__on_check_status_bulk_message)
        self.sio.on(GREETING, self.__on_greeting_message)
        self.sio.on(WORKFLOW_REGISTER, self.__on_workflow_reg_message)
        self.sio.on(WORKFLOW_REGISTER_ESSENCE, self.__on_workflow_reg_essence_message)
        self.sio.on(WORKFLOW_LIST, self.__on_workflow_list_message)
        self.sio.on(WORKFLOW_LIST_RUN, self.__on_workflow_list_run_message)
        self.sio.on(WORKFLOW_CHECK, self.__on_workflow_check_message)
        self.sio.on(WORKFLOW_REMOVE, self.__on_workflow_remove_message)
        self.sio.on(WORKFLOW_REMOVE_RUN, self.__on_workflow_remove_run_message)
        self.sio.on(WORKFLOW_REPORT_NEW_RUN, self.__on_workflow_report_new_run_message)
        self.sio.on(WORKFLOW_REPORT_RUN_STATUS, self.__on_workflow_report_run_status_message)
        self.sio.on(WORKFLOW_REPORT_RUN_STATUS_BULK, self.__on_workflow_report_run_status_bulk_message)

        self.handlers = {
            'connect': self.__noop_connect_handler,
            'disconnect': self.__noop_disconnect_handler,
            'kickstart': self.__noop_kickstart_handler,
            'check_status': self.__noop_check_status_handler,
            'check_status_bulk': self.__noop_check_status_bulk_handler,
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

    def __noop_kickstart_handler(self, workflow_id, workflow_run_id):
        self.logger.debug('no-op kickstart handler')

    def __noop_check_status_handler(self, workflow_id, workflow_run_id):
        self.logger.debug('no-op check-status handler')

    def __noop_check_status_bulk_handler(self, requests):
        self.logger.debug('no-op check-status-bulk handler')

    def __get_next_req_id(self):
        req_id = self.req_id
        self.req_id += 1
        return req_id

    def __on_greeting_message(self, data):
        self.logger.debug('received a gretting message from the server')

    def __on_kickstart_message(self, data):
        """
        Handler for a kickstart event
        REQ = {
            'workflow_id': <workflow_id>,
            'workflow_run_id': <workflow_run_id>
        }
        """
        self.logger.info('received a kickstart message from the server')
        workflow_id = data['workflow_id']
        workflow_run_id = data['workflow_run_id']

        self.logger.info(
            'a kickstart message - workflow_id (%s), workflow_run_id (%s)' %
            (workflow_id, workflow_run_id)
        )
        if workflow_id and workflow_run_id:
            handler = self.handlers['kickstart']
            if callable(handler):
                self.logger.info('calling a kickstart handler - %s' % handler)
                handler(workflow_id, workflow_run_id)

    def __on_check_status_message(self, data):
        """
        Handler for a check-status event
        REQ = {
            'workflow_id': <workflow_id>,
            'workflow_run_id': <workflow_run_id>
        }
        """
        self.logger.info('received a check-status message from the server')
        workflow_id = data['workflow_id']
        workflow_run_id = data['workflow_run_id']

        self.logger.info(
            'a check-status message - workflow_id (%s), workflow_run_id (%s)' %
            (workflow_id, workflow_run_id)
        )
        if workflow_id and workflow_run_id:
            handler = self.handlers['check_status']
            if callable(handler):
                self.logger.info('calling a check-status handler - %s' % handler)
                handler(workflow_id, workflow_run_id)

    def __on_check_status_bulk_message(self, data):
        """
        Handler for a check-status-bulk event
        REQ = [{
                'workflow_id': <workflow_id>,
                'workflow_run_id': <workflow_run_id>
            }, ...]
        """
        self.logger.info('received a check-status-bulk message from the server')
        if data:
            handler = self.handlers['check_status_bulk']
            if callable(handler):
                self.logger.info('calling a check-status handler - %s' % handler)
                handler(data)

    def __on_workflow_reg_message(self, data):
        self.__on_response(WORKFLOW_REGISTER, data)

    def __on_workflow_reg_essence_message(self, data):
        self.__on_response(WORKFLOW_REGISTER_ESSENCE, data)

    def __on_workflow_list_message(self, data):
        self.__on_response(WORKFLOW_LIST, data)

    def __on_workflow_list_run_message(self, data):
        self.__on_response(WORKFLOW_LIST_RUN, data)

    def __on_workflow_check_message(self, data):
        self.__on_response(WORKFLOW_CHECK, data)

    def __on_workflow_remove_message(self, data):
        self.__on_response(WORKFLOW_REMOVE, data)

    def __on_workflow_remove_run_message(self, data):
        self.__on_response(WORKFLOW_REMOVE_RUN, data)

    def __on_workflow_report_new_run_message(self, data):
        self.__on_response(WORKFLOW_REPORT_NEW_RUN, data)

    def __on_workflow_report_run_status_message(self, data):
        self.__on_response(WORKFLOW_REPORT_RUN_STATUS, data)

    def __on_workflow_report_run_status_bulk_message(self, data):
        self.__on_response(WORKFLOW_REPORT_RUN_STATUS_BULK, data)

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
        query_string = 'id=%s&type=workflow_manager&name=%s' % (self.name, self.name)
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

    def register_workflow(self, workflow):
        """
        Register a workflow.
        Workflow parameter is a workflow object
        """
        if workflow:
            result = self.__request(WORKFLOW_REGISTER, {
                'workflow': workflow
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
                'invalid arguments workflow (%s)' %
                json.dumps(workflow)
            )
            raise ClientInputError(
                'invalid arguments workflow (%s)' %
                json.dumps(workflow)
            )

    def register_workflow_essence(self, essence):
        """
        Register a workflow by essence.
        """
        if essence:
            result = self.__request(WORKFLOW_REGISTER_ESSENCE, {
                'essence': essence
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
                'invalid arguments workflow essence (%s)' %
                json.dumps(essence)
            )
            raise ClientInputError(
                'invalid arguments workflow essence (%s)' %
                json.dumps(essence)
            )

    def list_workflows(self):
        """
        List workflows.
        """
        result = self.__request(WORKFLOW_LIST, {})
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

    def list_workflow_runs(self):
        """
        List workflow runs.
        """
        result = self.__request(WORKFLOW_LIST_RUN, {})
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

    def check_workflow(self, workflow_id):
        """
        Check a workflow.
        """
        if workflow_id:
            result = self.__request(WORKFLOW_CHECK, {
                'workflow_id': workflow_id
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
                'invalid arguments workflow_id (%s)' %
                workflow_id
            )
            raise ClientInputError(
                'invalid arguments workflow_id (%s)' %
                workflow_id
            )

    def remove_workflow(self, workflow_id):
        """
        Remove a workflow.
        """
        if workflow_id:
            result = self.__request(WORKFLOW_REMOVE, {
                'workflow_id': workflow_id
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
                'invalid arguments workflow_id (%s)' %
                workflow_id
            )
            raise ClientInputError(
                'invalid arguments workflow_id (%s)' %
                workflow_id
            )

    def remove_workflow_run(self, workflow_id, workflow_run_id):
        """
        Remove a workflow run.
        """
        if workflow_id and workflow_run_id:
            result = self.__request(WORKFLOW_REMOVE_RUN, {
                'workflow_id': workflow_id,
                'workflow_run_id': workflow_run_id
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
                'invalid arguments workflow_id (%s) workflow_run_id (%s)' %
                (workflow_id, workflow_run_id)
            )
            raise ClientInputError(
                'invalid arguments workflow_id (%s) workflow_run_id (%s)' %
                (workflow_id, workflow_run_id)
            )

    def report_new_workflow_run(self, workflow_id, workflow_run_id):
        """
        Report a new workflow run
        """
        if workflow_id and workflow_run_id:
            result = self.__request(WORKFLOW_REPORT_NEW_RUN, {
                'workflow_id': workflow_id,
                'workflow_run_id': workflow_run_id
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
                'invalid arguments workflow_id (%s), workflow_run_id (%s)' %
                (workflow_id, workflow_run_id)
            )
            raise ClientInputError(
                'invalid arguments workflow_id (%s), workflow_run_id (%s)' %
                (workflow_id, workflow_run_id)
            )

    def report_workflow_run_status(self, workflow_id, workflow_run_id, status):
        """
        Report status of a workflow run
        """
        if workflow_id and workflow_run_id and status:
            result = self.__request(WORKFLOW_REPORT_RUN_STATUS, {
                'workflow_id': workflow_id,
                'workflow_run_id': workflow_run_id,
                'status': status
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
                'invalid arguments workflow_id (%s), workflow_run_id (%s), status (%s)' %
                (workflow_id, workflow_run_id, status)
            )
            raise ClientInputError(
                'invalid arguments workflow_id (%s), workflow_run_id (%s), status (%s)' %
                (workflow_id, workflow_run_id, status)
            )

    def report_workflow_run_status_bulk(self, data):
        """
        Report statuses of a workflow run
        """

        if data:
            result = self.__request(WORKFLOW_REPORT_RUN_STATUS_BULK, {
                'data': data
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
                'invalid arguments data (%s)' %
                json.dumps(data)
            )
            raise ClientInputError(
                'invalid arguments data (%s)' %
                json.dumps(data)
            )
