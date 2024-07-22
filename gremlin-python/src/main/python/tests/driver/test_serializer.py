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
from gremlin_python.structure.io import graphbinaryV4


__author__ = 'David M. Brown'


def test_graphbinary_serializer_v4(graphbinary_serializer_v4):
    assert graphbinary_serializer_v4.version == b"application/vnd.graphbinary-v4.0"
    assert isinstance(graphbinary_serializer_v4._graphbinary_reader, graphbinaryV4.GraphBinaryReader)
