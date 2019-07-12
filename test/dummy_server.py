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

from __future__ import absolute_import
import socketio
import psutil
import time
import datetime

from threading import Timer
from gevent import pywsgi
from geventwebsocket.handler import WebSocketHandler
from multiprocessing import Process
from multistructlog import create_logger
from cord_workflow_controller_client.probe import GREETING
from cord_workflow_controller_client.manager \
    import (WORKFLOW_KICKSTART,
            WORKFLOW_REGISTER, WORKFLOW_REGISTER_ESSENCE, WORKFLOW_LIST, WORKFLOW_LIST_RUN,
            WORKFLOW_CHECK, WORKFLOW_REMOVE, WORKFLOW_REMOVE_RUN, WORKFLOW_NOTIFY_NEW_RUN)
from cord_workflow_controller_client.workflow_run \
    import (WORKFLOW_RUN_NOTIFY_EVENT,
            WORKFLOW_RUN_UPDATE_STATUS, WORKFLOW_RUN_COUNT_EVENTS, WORKFLOW_RUN_FETCH_EVENT)


"""
Run a dummy socket.io server as a separate process.
serve_forever() blocks until the process is killed,
so I had to use multi-process approach.
"""

log = create_logger()

# Socket IO
sio = None

manager_clients = {}
workflows = {}
workflow_essences = {}
workflow_runs = {}
workflow_run_clients = {}
seq_no = 1


class repeatableTimer():
    def __init__(self, time, handler, arg):
        self.time = time
        self.handler = handler
        self.arg = arg
        self.thread = Timer(self.time, self.on_tick)

    def on_tick(self):
        self.handler(self.arg)
        self.thread = Timer(self.time, self.on_tick)
        self.thread.start()

    def start(self):
        self.thread.start()

    def cancel(self):
        self.thread.cancel()


def make_query_string_dict(query_string):
    obj = {}
    params = query_string.split('&')
    for param in params:
        kv = param.split('=')
        key = kv[0]
        val = kv[1]
        obj[key] = val

    return obj


def _send_kickstart_event(sid):
    global seq_no

    workflow_id = 'dummy_workflow_%d' % seq_no
    workflow_run_id = 'dummy_workflow_run_%d' % seq_no

    seq_no += 1
    log.info('sending a kickstart event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_KICKSTART,
        data={
            'workflow_id': workflow_id,
            'workflow_run_id': workflow_run_id,
            'timestamp': str(datetime.datetime.now())
        },
        room=sid
    )


def _send_notify_event(sid):
    global seq_no

    topic = 'topic_%s' % seq_no
    message = {
        'sample_key': 'sample_value'
    }
    seq_no += 1

    run_client = workflow_run_clients[sid]
    if run_client:
        workflow_run_id = run_client['workflow_run_id']
        workflow_run = workflow_runs[workflow_run_id]
        if workflow_run:
            workflow_run['queue'].append({
                'topic': topic,
                'message': message
            })

            log.info('sending a notify event to sid %s' % sid)
            sio.emit(
                event=WORKFLOW_RUN_NOTIFY_EVENT,
                data={
                    'topic': topic,
                    'timestamp': str(datetime.datetime.now())
                },
                room=sid
            )


def _handle_event_connect(sid, query):
    sio.emit(GREETING, {})

    global last_client_action_time
    last_client_action_time = datetime.datetime.now

    # if the client is a manager, send kickstart events every 3 sec
    if query['type'] == 'workflow_manager':
        log.info('manager (%s) is connected' % sid)
        kickstart_timer = repeatableTimer(2, _send_kickstart_event, sid)
        manager_clients[sid] = {
            'kickstart_timer': kickstart_timer
        }

        kickstart_timer.start()
    elif query['type'] == 'workflow_run':
        log.info('workflow run (%s) is connected' % sid)
        notify_event_timer = repeatableTimer(2, _send_notify_event, sid)
        workflow_run_clients[sid] = {
            'workflow_id': query['workflow_id'],
            'workflow_run_id': query['workflow_run_id'],
            'notify_event_timer': notify_event_timer
        }

        notify_event_timer.start()


def _handle_event_disconnect(sid):
    if sid in manager_clients:
        log.info('manager (%s) is disconnected' % sid)
        if manager_clients[sid]['kickstart_timer']:
            manager_clients[sid]['kickstart_timer'].cancel()

        del manager_clients[sid]

    if sid in workflow_run_clients:
        log.info('workflow run (%s) is disconnected' % sid)
        if workflow_run_clients[sid]['notify_event_timer']:
            workflow_run_clients[sid]['notify_event_timer'].cancel()

        del workflow_run_clients[sid]

    global last_client_action_time
    last_client_action_time = datetime.datetime.now


def _get_req_id(body):
    req_id = 101010
    if 'req_id' in body:
        req_id = int(body['req_id'])
    return req_id


def _handle_event_workflow_reg(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'workflow' in body:
        workflow = body['workflow']
        workflow_id = workflow['id']

        if workflow_id in workflows:
            # already exist
            data['error'] = True
            data['result'] = False
            data['message'] = 'workflow is already registered'
        else:
            log.info('manager (%s) registers a workflow (%s)' % (sid, workflow_id))
            workflows[workflow_id] = workflow

            data['error'] = False
            data['result'] = True
    else:
        data['error'] = True
        data['result'] = False
        data['message'] = 'workflow is not in the message body'

    log.info('returning a result for workflow register event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_REGISTER,
        data=data,
        room=sid
    )


def _handle_event_workflow_reg_essence(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'essence' in body:
        essence = body['essence']
        for wid in essence:
            workflow_essence = essence[wid]
            if 'dag' in workflow_essence and 'dag_id' in workflow_essence['dag']:
                dag = workflow_essence['dag']
                workflow_id = dag['dag_id']

                if workflow_id in workflow_essences or workflow_id in workflows:
                    # already exist
                    data['error'] = True
                    data['result'] = False
                    data['message'] = 'workflow is already registered'
                else:
                    log.info('manager (%s) registers a workflow (%s)' % (sid, workflow_id))
                    workflow_essences[workflow_id] = workflow_essence

                    data['error'] = False
                    data['result'] = True
            else:
                data['error'] = True
                data['result'] = False
                data['message'] = 'essence is not in the message body'
    else:
        data['error'] = True
        data['result'] = False
        data['message'] = 'essence is not in the message body'

    log.info('returning a result for workflow essence register event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_REGISTER_ESSENCE,
        data=data,
        room=sid
    )


def _handle_event_workflow_list(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    workflow_ids = []

    for workflow_id in workflows:
        workflow_ids.append(workflow_id)

    for workflow_id in workflow_essences:
        workflow_ids.append(workflow_id)

    data['error'] = False
    data['result'] = workflow_ids

    log.info('returning a result for workflow list event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_LIST,
        data=data,
        room=sid
    )


def _handle_event_workflow_run_list(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    workflow_run_ids = []

    for workflow_run_id in workflow_runs:
        workflow_run_ids.append(workflow_run_id)

    data['error'] = False
    data['result'] = workflow_run_ids

    log.info('returning a result for workflow run list event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_LIST_RUN,
        data=data,
        room=sid
    )


def _handle_event_workflow_check(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'workflow_id' in body:
        workflow_id = body['workflow_id']
        if workflow_id in workflows:
            data['error'] = False
            data['result'] = True
        else:
            data['error'] = False
            data['result'] = False
    else:
        data['error'] = True
        data['result'] = False
        data['message'] = 'workflow_id is not in the message body'

    log.info('returning a result for workflow check event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_CHECK,
        data=data,
        room=sid
    )


def _handle_event_workflow_remove(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'workflow_id' in body:
        workflow_id = body['workflow_id']
        if workflow_id in workflows:

            hasWorkflowRuns = False
            for workflow_run_id in workflow_runs:
                workflow_run = workflow_runs[workflow_run_id]
                wid = workflow_run['workflow_id']
                if wid == workflow_id:
                    # there is a workflow run for the workflow id
                    hasWorkflowRuns = True
                    break

            if hasWorkflowRuns:
                data['error'] = False
                data['result'] = False
            else:
                del workflows[workflow_id]

                data['error'] = False
                data['result'] = True
        else:
            data['error'] = False
            data['result'] = False
    else:
        data['error'] = True
        data['result'] = False
        data['message'] = 'workflow_id is not in the message body'

    log.info('returning a result for workflow remove event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_REMOVE,
        data=data,
        room=sid
    )


def _handle_event_workflow_run_remove(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'workflow_id' in body and 'workflow_run_id' in body:
        # workflow_id = body['workflow_id']
        workflow_run_id = body['workflow_run_id']

        if workflow_run_id in workflow_runs:
            del workflow_runs[workflow_run_id]

            data['error'] = False
            data['result'] = True
        else:
            data['error'] = False
            data['result'] = False
    else:
        data['error'] = True
        data['result'] = False
        data['message'] = 'workflow_id or workflow_run_id is not in the message body'

    log.info('returning a result for workflow run remove event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_REMOVE_RUN,
        data=data,
        room=sid
    )


def _handle_event_new_workflow_run(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'workflow_id' in body and 'workflow_run_id' in body:
        workflow_id = body['workflow_id']
        workflow_run_id = body['workflow_run_id']

        log.info('manager (%s) started a new workflow (%s), workflow_run (%s)' % (sid, workflow_id, workflow_run_id))
        workflow_runs[workflow_run_id] = {
            'workflow_id': workflow_id,
            'workflow_run_id': workflow_run_id,
            'queue': []
        }

        data['error'] = False
        data['result'] = True
    else:
        data['error'] = True
        data['result'] = False
        data['message'] = 'workflow_id or workflow_run_id is not in the message body'

    log.info('returning a result for a new workflow run event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_NOTIFY_NEW_RUN,
        data=data,
        room=sid
    )


def _handle_event_workflow_run_update_status(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'workflow_id' in body and 'workflow_run_id' in body and 'task_id' in body and 'status' in body:
        # workflow_id = body['workflow_id']
        workflow_run_id = body['workflow_run_id']
        task_id = body['task_id']
        status = body['status']

        if workflow_run_id in workflow_runs:
            workflow_run = workflow_runs[workflow_run_id]
            workflow_run[task_id] = status

            data['error'] = False
            data['result'] = True
        else:
            data['error'] = True
            data['result'] = False
            data['message'] = 'cannot find workflow run'
    else:
        data['error'] = True
        data['result'] = False
        data['message'] = 'workflow_id, workflow_run_id, task_id or status is not in the message body'

    log.info('returning a result for workflow run update status event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_RUN_UPDATE_STATUS,
        data=data,
        room=sid
    )


def _handle_event_workflow_run_count_events(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'workflow_id' in body and 'workflow_run_id' in body:
        # workflow_id = body['workflow_id']
        workflow_run_id = body['workflow_run_id']

        if workflow_run_id in workflow_runs:
            workflow_run = workflow_runs[workflow_run_id]
            queue = workflow_run['queue']
            count = len(queue)

            data['error'] = False
            data['result'] = count
        else:
            data['error'] = True
            data['result'] = 0
            data['message'] = 'cannot find workflow run'
    else:
        data['error'] = True
        data['result'] = 0
        data['message'] = 'workflow_id, workflow_run_id, task_id or status is not in the message body'

    log.info('returning a result for workflow run count events to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_RUN_COUNT_EVENTS,
        data=data,
        room=sid
    )


def _handle_event_workflow_run_fetch_event(sid, body):
    data = {
        'req_id': _get_req_id(body)
    }

    if 'workflow_id' in body and 'workflow_run_id' in body and 'task_id' in body and 'topic' in body:
        # workflow_id = body['workflow_id']
        workflow_run_id = body['workflow_run_id']
        # task_id = body['task_id']
        topic = body['topic']

        if workflow_run_id in workflow_runs:
            workflow_run = workflow_runs[workflow_run_id]
            queue = workflow_run['queue']

            event = None
            for idx in range(len(queue)):
                if queue[idx]['topic'] == topic:
                    # found
                    event = queue.pop(idx)
                    break

            if event:
                data['error'] = False
                data['result'] = event
            else:
                data['error'] = False
                data['result'] = {}
        else:
            data['error'] = False
            data['result'] = False
            data['message'] = 'cannot find workflow run'
    else:
        data['error'] = True
        data['result'] = False
        data['message'] = 'workflow_id, workflow_run_id, task_id or topic is not in the message body'

    log.info('returning a result for workflow run fetch event to sid %s' % sid)
    sio.emit(
        event=WORKFLOW_RUN_FETCH_EVENT,
        data=data,
        room=sid
    )


def _handle_event(event, sid, body):
    log.info('event %s - body %s (%s)' % (event, body, type(body)))


class ServerEventHandler(socketio.namespace.Namespace):
    def trigger_event(self, event, *args):
        sid = args[0]
        if event == 'connect':
            querystr = args[1]['QUERY_STRING']
            query = make_query_string_dict(querystr)
            _handle_event_connect(sid, query)
        elif event == 'disconnect':
            _handle_event_disconnect(sid)

        # manager
        elif event == WORKFLOW_NOTIFY_NEW_RUN:
            _handle_event_new_workflow_run(sid, args[1])
        elif event == WORKFLOW_REGISTER_ESSENCE:
            _handle_event_workflow_reg_essence(sid, args[1])
        elif event == WORKFLOW_REGISTER:
            _handle_event_workflow_reg(sid, args[1])
        elif event == WORKFLOW_LIST:
            _handle_event_workflow_list(sid, args[1])
        elif event == WORKFLOW_LIST_RUN:
            _handle_event_workflow_run_list(sid, args[1])
        elif event == WORKFLOW_CHECK:
            _handle_event_workflow_check(sid, args[1])
        elif event == WORKFLOW_REMOVE:
            _handle_event_workflow_remove(sid, args[1])
        elif event == WORKFLOW_REMOVE_RUN:
            _handle_event_workflow_run_remove(sid, args[1])

        # workflow run
        elif event == WORKFLOW_RUN_UPDATE_STATUS:
            _handle_event_workflow_run_update_status(sid, args[1])
        elif event == WORKFLOW_RUN_COUNT_EVENTS:
            _handle_event_workflow_run_count_events(sid, args[1])
        elif event == WORKFLOW_RUN_FETCH_EVENT:
            _handle_event_workflow_run_fetch_event(sid, args[1])
        else:
            _handle_event(event, args[0], args[1])


def _run(port):
    global sio
    sio = socketio.Server(ping_timeout=5, ping_interval=1)
    app = socketio.WSGIApp(sio)
    sio.register_namespace(ServerEventHandler('/'))

    server = pywsgi.WSGIServer(
        ('', port),
        app,
        handler_class=WebSocketHandler
    )

    server.serve_forever()


def start(port):
    p = Process(target=_run, args=(port, ))
    p.start()
    time.sleep(3)

    log.info('Dummy server is started!')
    return p


def stop(p):
    log.info('Stopping dummy server!')

    try:
        process = psutil.Process(p.pid)
        for proc in process.children(recursive=True):
            proc.kill()
        process.kill()
        p.join()
    except psutil.NoSuchProcess:
        pass

    # clean-up
    global sio, manager_clients, workflow_runs, seq_no
    sio = None
    manager_clients = {}
    workflow_runs = {}
    seq_no = 1

    time.sleep(3)

    log.info('Dummy server is stopped!')
