#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.process.traversal import TraversalStrategies
import warnings


class Graph(object):

    def __repr__(self):
        return "graph[]"


class Element(object):
    def __init__(self, id, label, properties=None):
        self.id = id
        self.label = label
        self.properties = [] if properties is None else properties

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class Vertex(Element):
    def __init__(self, id, label="vertex", properties=None):
        Element.__init__(self, id, label, properties)

    def __repr__(self):
        return "v[" + str(self.id) + "]"


class Edge(Element):
    def __init__(self, id, outV, label, inV, properties=None):
        Element.__init__(self, id, label, properties)
        self.outV = outV
        self.inV = inV

    def __repr__(self):
        return "e[" + str(self.id) + "][" + str(self.outV.id) + "-" + self.label + "->" + str(self.inV.id) + "]"


class VertexProperty(Element):
    def __init__(self, id, label, value, vertex, properties=None):
        Element.__init__(self, id, label, properties)
        self.value = value
        self.key = self.label
        self.vertex = vertex

    def __repr__(self):
        return "vp[" + str(self.label) + "->" + str(self.value)[0:20] + "]"


class Property(object):
    def __init__(self, key, value, element):
        self.key = key
        self.value = value
        self.element = element

    def __repr__(self):
        return "p[" + str(self.key) + "->" + str(self.value)[0:20] + "]"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               self.key == other.key and \
               self.value == other.value and \
               self.element == other.element

    def __hash__(self):
        return hash(self.key) + hash(self.value)


class Path(object):
    def __init__(self, labels, objects):
        self.labels = labels
        self.objects = objects

    def __repr__(self):
        return "path[" + ", ".join(map(str, self.objects)) + "]"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.objects == other.objects and self.labels == other.labels

    def __hash__(self):
        return hash(str(self.objects)) + hash(str(self.labels))

    def __getitem__(self, key):
        if isinstance(key, str):
            objects = []
            for i, labels in enumerate(self.labels):
                if key in labels:
                    objects.append(self.objects[i])
            if 0 == len(objects):
                raise KeyError("The step with label " + key + " does not exist")
            return objects if len(objects) > 1 else objects[0]
        elif isinstance(key, int):
            return self.objects[key]
        else:
            raise TypeError("The path access key must be either a string label or integer index")

    def __len__(self):
        return len(self.objects)
