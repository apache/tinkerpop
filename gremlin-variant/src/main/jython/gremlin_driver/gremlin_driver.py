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
from abc import abstractmethod

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


class Traverser(object):
    def __init__(self, object, bulk):
        self.object = object
        self.bulk = bulk
    def __repr__(self):
        return str(self.object)


class RemoteConnection(object):
    def __init__(self, url, scriptEngine):
        self.url = url
        self.scriptEngine = scriptEngine

    @abstractmethod
    def submit(self, script):
        print "sending " + script + " to GremlinServer..."
        return iter([])
