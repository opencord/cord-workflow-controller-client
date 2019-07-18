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
import unittest
import time
import os
import json
from cord_workflow_controller_client.manager import Manager
from cord_workflow_controller_client.workflow_run import WorkflowRun
from multistructlog import create_logger
from .dummy_server import start as server_start, stop as server_stop

log = create_logger()
code_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


def read_json_file(filename):
    if filename:
        with open(filename, 'r') as f:
            return json.load(f)
    return None


class TestWorkflowRun(unittest.TestCase):
    """
    Try to connect to a local fake Controller Service as a Manager.
    """

    def setUp(self):
        self.kickstarted_workflows = {}
        self.notifications = []

        self.server = server_start(17080)
        self.manager = Manager(logger=log)
        self.manager.connect('http://localhost:17080')

        essence_path = os.path.join(code_dir, "hello_workflow.json")
        essence = read_json_file(essence_path)
        self.manager.register_workflow_essence(essence)
        self.manager.report_new_workflow_run('hello_workflow', 'hello_workflow_123')

        # wait for 2 seconds for registering a new workflow run
        time.sleep(2)

    def tearDown(self):
        self.manager.disconnect()
        self.manager = None

        server_stop(self.server)
        self.server = None

        self.kickstarted_workflows = {}
        self.notifications = []

    def test_connect(self):
        """
        This tests if workflow run client can connect to a socket.io server properly.
        """
        succeed = False
        try:
            run = WorkflowRun('hello_workflow', 'hello_workflow_123')
            run.connect('http://localhost:17080')

            time.sleep(1)

            run.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')

    def test_count_events(self):
        """
        This tests if workflow run client can retrieve the number of events.
        """
        succeed = False
        try:
            run = WorkflowRun('hello_workflow', 'hello_workflow_123')
            run.connect('http://localhost:17080')

            # dummy server generates a message for every 2 seconds
            # we wait 6 seconds to queue at least 2 messages
            time.sleep(6)

            count = run.count_events()

            run.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')
            self.assertTrue(count >= 2, 'There must be more than 2 events queued')

    def test_notify_event(self):
        """
        This tests if workflow run client can get a noficitation for events.
        """
        succeed = False
        try:
            run = WorkflowRun('hello_workflow', 'hello_workflow_123')
            run.connect('http://localhost:17080')

            def on_notification(workflow_id, workflow_run_id, topic):
                self.notifications.append({
                    'workflow_id': workflow_id,
                    'workflow_run_id': workflow_run_id,
                    'topic': topic
                })

            run.set_handlers({'notify': on_notification})

            # dummy server generates a message for every 2 seconds
            # we wait 6 seconds to get at least 2 notifications
            time.sleep(6)

            count = len(self.notifications)

            run.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')
            self.assertTrue(count >= 2, 'There must be more than 2 notifications received')

    def test_get_events(self):
        """
        This tests if workflow run client can retrieve events.
        """
        succeed = False
        try:
            run = WorkflowRun('hello_workflow', 'hello_workflow_123')
            run.connect('http://localhost:17080')

            def on_notification(workflow_id, workflow_run_id, topic):
                self.notifications.append({
                    'workflow_id': workflow_id,
                    'workflow_run_id': workflow_run_id,
                    'topic': topic
                })

            run.set_handlers({'notify': on_notification})

            # dummy server generates a message for every 2 seconds
            # we wait 6 seconds to queue at least 2 messages
            time.sleep(6)

            count_notified = len(self.notifications)
            count_queued = run.count_events()

            self.assertTrue(count_notified >= 2, 'There must be more than 2 events notified')
            self.assertTrue(count_queued >= 2, 'There must be more than 2 events queued')

            # count_notified and count_queued may not have the same number temporarily
            for _ in range(count_notified):
                notification = self.notifications.pop(0)
                topic = notification['topic']
                event = run.fetch_event('task123', topic)

            self.assertTrue('topic' in event, 'event should not be empty')
            self.assertTrue(event['topic'] == topic, 'event should be retrieved by topic')
            self.assertTrue(len(event['message']) > 0, 'there must be some messages')

            run.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')


if __name__ == "__main__":
    unittest.main()
