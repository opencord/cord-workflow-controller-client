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
from cord_workflow_controller_client.probe import Probe
from multistructlog import create_logger
from .dummy_server import start as server_start, stop as server_stop

log = create_logger()


class TestProbe(unittest.TestCase):
    """
    Try to connect to a local fake Controller Service as a Probe.
    """

    def setUp(self):
        self.server = server_start(17080)

    def tearDown(self):
        server_stop(self.server)
        self.server = None

    def test_connect(self):
        """
        This tests if Probe client can connect to a socket.io server properly.
        """
        succeed = False
        try:
            probe = Probe(logger=log)
            probe.connect('http://localhost:17080')

            time.sleep(1)

            probe.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')

    def test_emit_string(self):
        """
        This tests if Probe client can emit an event.
        """
        succeed = False
        try:
            probe = Probe(logger=log)
            probe.connect('http://localhost:17080')

            probe.emit_event('xos.test.event', 'string message - hello')
            time.sleep(1)

            probe.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')

    def test_emit_json(self):
        """
        This tests if Probe client can emit an event with a dict (json) object.
        """
        succeed = False
        try:
            probe = Probe(logger=log)
            probe.connect('http://localhost:17080')

            probe.emit_event(
                'xos.test.event',
                {
                    'str_key': 'value',
                    'int_key': 32335
                }
            )
            time.sleep(1)

            probe.disconnect()
            succeed = True
        finally:
            self.assertTrue(succeed, 'Finished with error')


if __name__ == "__main__":
    unittest.main()
