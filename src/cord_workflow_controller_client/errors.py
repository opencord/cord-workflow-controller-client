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
Errors
"""


class ClientError(Exception):
    """
    Base class for exceptions in this module.
    """
    pass


class ClientRPCError(ClientError):
    """
    Raised when an RPC call failed.

    Attributes:
        req_id -- request id
        message -- explanation of the reason why the RPC call is failed
    """
    def __init__(self, req_id, message):
        self.req_id = req_id
        self.message = message


class ClientInputError(ClientError):
    """
    Raised when input parameters are missing or wrong.

    Attributes:
        message -- explanation of the reason why the RPC call is failed
    """
    def __init__(self, message):
        self.message = message


class ClientResponseError(ClientError):
    """
    Raised when error is returned

    Attributes:
        message -- explanation of the reason why the request is failed
    """
    def __init__(self, message):
        self.message = message
