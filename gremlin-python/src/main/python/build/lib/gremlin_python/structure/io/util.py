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

import re


class SymbolUtil:
    camel_pattern = re.compile(r'(?<!^)(?=[A-Z])')

    @classmethod
    def to_snake_case(cls, symbol):
        return cls.camel_pattern.sub('_', symbol).lower()

    @classmethod
    def to_camel_case(cls, symbol):
        u = ''
        first = True
        for segment in symbol.split('_'):
            u += segment if first else segment.title()
            first = False

        return u


class HashableDict(dict):
    def __hash__(self):
        try:
            return hash(tuple(sorted(self.items())))
        except:
            return hash(tuple(sorted(str(x) for x in self.items())))

    @classmethod
    def of(cls, o):
        if isinstance(o, (tuple, set, list)):
            return tuple([cls.of(e) for e in o])
        elif not isinstance(o, (dict, HashableDict)):
            return o

        new_o = HashableDict()
        for k, v in o.items():
            if isinstance(k, (set, list)):
                new_o[tuple(k)] = cls.of(v)
            else:
                new_o[k] = cls.of(v)
        return new_o

