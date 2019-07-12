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
Count-down latch
"""

import threading
import time


class CountDownLatch(object):
    def __init__(self, count=1):
        self.count = count
        self.condition = threading.Condition()

    def count_down(self, count=1):
        self.condition.acquire()
        self.count -= count
        if self.count <= 0:
            self.condition.notifyAll()
        self.condition.release()

    def wait(self, timeout=0):
        self.condition.acquire()
        start_time = time.time()

        while self.count > 0:
            self.condition.wait(timeout)
            cur_time = time.time()
            if cur_time - start_time >= timeout:
                break

        self.condition.release()
        if self.count <= 0:
            return True
        else:
            # timeout
            return False

    def get_count(self):
        return self.count
