'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
'''
import abc

import six


__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


@six.add_metaclass(abc.ABCMeta)
class RemoteConnection(object):

    def __init__(self, url, traversal_source):
        self._url = url
        self._traversal_source = traversal_source

    @property
    def url(self):
        return self._url

    @property
    def traversal_source(self):
        return self._traversal_source

    @abc.abstractmethod
    def submit(self, target_language, bytecode):
        print "sending " + bytecode + " to GremlinServer..."
        return RemoteResponse(iter([]), {})


class RemoteResponse(object):

    def __init__(self, traversers, side_effects):
        self.traversers = traversers
        self.side_effects = side_effects
