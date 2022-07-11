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

from types import FunctionType
from aenum import Enum


class long(int):
    pass


class bigint(int):
    pass


class short(int):
    pass


FloatType = float
ShortType = short
IntType = int
LongType = long
BigIntType = bigint
TypeType = type
ListType = list
DictType = dict
SetType = set
ByteBufferType = bytes


class timestamp(float):
    """
    In Python a timestamp is simply a float. This dummy class (similar to long), allows users to wrap a float
    in a GLV script to make sure the value is serialized as a Gremlin timestamp.
    """
    pass


class SingleByte(int):
    """
    Provides a way to pass a single byte via Gremlin.
    """
    def __new__(cls, b):
        if -128 <= b < 128:
            return int.__new__(cls, b)
        else:
            raise ValueError("value must be between -128 and 127 inclusive")


class SingleChar(str):
    """
    Provides a way to pass a single character via Gremlin.
    """
    def __new__(cls, c):
        if len(c) == 1:
            return str.__new__(cls, c)
        else:
            raise ValueError("string must contain a single character")


class GremlinType(object):
    """
    Provides a way to represent a "Java class" for Gremlin.
    """
    def __init__(self, gremlin_type):
        self.gremlin_type = gremlin_type


class BigDecimal(object):
    def __init__(self, scale, unscaled_value):
        self.scale = scale
        self.unscaled_value = unscaled_value


staticMethods = {}
staticEnums = {}
default_lambda_language = "gremlin-groovy"


def add_static(key, value):
    if isinstance(value, Enum):
        staticEnums[key] = value
    else:
        staticMethods[key] = value


def load_statics(global_dict):
    for key in staticMethods:
        global_dict[key] = staticMethods[key]
    for key in staticEnums:
        global_dict[key] = staticEnums[key]


def unload_statics(global_dict):
    for key in staticMethods:
        if key in global_dict:
            del global_dict[key]
    for key in staticEnums:
        if key in global_dict:
            del global_dict[key]
