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
import atexit
from .dummy_server import stop as server_stop

dummy_servers = {}


def cleanup_dummy_servers():
    for pid in dummy_servers:
        s = dummy_servers[pid]
        server_stop(s)
        del dummy_servers[pid]


def register_dummy_server_cleanup(s):
    if s.pid not in dummy_servers:
        dummy_servers[s.pid] = s


def unregister_dummy_server_cleanup(s):
    if s.pid in dummy_servers:
        del dummy_servers[s.pid]


atexit.register(cleanup_dummy_servers)
