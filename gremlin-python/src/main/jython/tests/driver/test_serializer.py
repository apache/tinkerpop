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
from gremlin_python.structure.io import graphsonV2d0
from gremlin_python.structure.io import graphsonV3d0


__author__ = 'David M. Brown'


def test_graphson_serialzier_v2(graphson_serializer_v2):
    assert graphson_serializer_v2.version == b"application/vnd.gremlin-v2.0+json"
    assert isinstance(graphson_serializer_v2._graphson_reader, graphsonV2d0.GraphSONReader)
    assert isinstance(graphson_serializer_v2.standard._graphson_writer, graphsonV2d0.GraphSONWriter)
    assert isinstance(graphson_serializer_v2.traversal._graphson_writer, graphsonV2d0.GraphSONWriter)


def test_graphson_serialzier_v3(graphson_serializer_v3):
    assert graphson_serializer_v3.version == b"application/vnd.gremlin-v3.0+json"
    assert isinstance(graphson_serializer_v3._graphson_reader, graphsonV3d0.GraphSONReader)
    assert isinstance(graphson_serializer_v3.standard._graphson_writer, graphsonV3d0.GraphSONWriter)
    assert isinstance(graphson_serializer_v3.traversal._graphson_writer, graphsonV3d0.GraphSONWriter)
