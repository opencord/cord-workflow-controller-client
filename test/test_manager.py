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
from multistructlog import create_logger
from .dummy_server import start as server_start, stop as server_stop
from .dummy_server_util import register_dummy_server_cleanup, unregister_dummy_server_cleanup

log = create_logger()
code_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


def read_json_file(filename):
    if filename:
        with open(filename, 'r') as f:
            return json.load(f)
    return None


class TestManager(unittest.TestCase):
    """
    Try to connect to a local fake Controller Service as a Manager.
    """

    def setUp(self):
        self.server = server_start(17080)
        self.kickstarted_workflows = {}
        register_dummy_server_cleanup(self.server)

    def tearDown(self):
        server_stop(self.server)
        unregister_dummy_server_cleanup(self.server)
        self.server = None
        self.kickstarted_workflows = {}

    def test_connect(self):
        """
        This tests if Manager client can connect to a socket.io server properly.
        """
        succeed = False
        try:
            manager = Manager(logger=log)
            manager.connect('http://localhost:17080')

            time.sleep(1)

            manager.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')

    def test_kickstart(self):
        """
        This tests if Manager client can receive a kickstart event.
        """
        succeed = False

        try:
            manager = Manager(logger=log)
            manager.connect('http://localhost:17080')

            def on_kickstart(workflow_id, workflow_run_id):
                self.kickstarted_workflows[workflow_id] = {
                    'workflow_id': workflow_id,
                    'workflow_run_id': workflow_run_id
                }
                manager.report_new_workflow_run(workflow_id, workflow_run_id)

            manager.set_handlers({'kickstart': on_kickstart})

            # dummy server sends a kickstart message for every 2 seconds
            # we wait 6 seconds to receive at least 2 messages
            time.sleep(6)

            manager.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')
            self.assertTrue(len(self.kickstarted_workflows) >= 2, 'Kickstart event is not handled')

    def test_workflow_essence_register(self):
        """
        This tests if Manager client can register workflow essence.
        """
        succeed = False
        essence_path = os.path.join(code_dir, "hello_workflow.json")
        essence = read_json_file(essence_path)

        try:
            manager = Manager(logger=log)
            manager.connect('http://localhost:17080')

            # the command is synchronous
            result = manager.register_workflow_essence(essence)

            manager.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')
            self.assertTrue(result, 'workflow essence register failed')


if __name__ == "__main__":
    unittest.main()
