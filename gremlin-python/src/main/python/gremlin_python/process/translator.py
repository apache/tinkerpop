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

"""
Class that can turn traversals back into Gremlin Groovy format text queries.
Those queries can then be run in the Gremlin console or using the GLV submit(<String>) API or
sent to any TinkerPop compliant HTTP endpoint.
"""
__author__ = 'Kelvin R. Lawrence (gfxman)'

from gremlin_python.process.graph_traversal import __
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.strategies import *
from datetime import datetime


class Translator:
    """
    Turn a bytecode object back into a textual query (Gremlin Groovy script).
    """

    # Dictionary used to reverse-map token IDs to strings
    options = {
      WithOptions.tokens: 'tokens',
      WithOptions.none: 'none',
      WithOptions.ids: 'ids',
      WithOptions.labels: 'labels',
      WithOptions.keys: 'keys',
      WithOptions.values: 'values',
      WithOptions.all: 'all',
      WithOptions.indexer: 'indexer',
      WithOptions.list: 'list',
      WithOptions.map: 'map'                           
    }

    def __init__(self, traversal_source=None):
        self.traversal_source = traversal_source

    def get_traversal_source(self):
        return self.traversal_source

    def get_target_language(self):
        return "gremlin-groovy"

    def of(self,traversal_source):
        self.traversal_source = traversal_source
        return self

    # Do any needed special processing for the representation
    # of strings and dates.
    def fixup(self, v):
        if type(v) == str:
            return f'\'{v}\''
        elif type(v) == datetime:
            return self.process_date(v)
        else:
            return str(v)
    
    # Turn a Python datetime into the equivalent new Date(...)
    def process_date(self, date):
        y = date.year - 1900
        mo = date.month
        d = date.day
        h = date.hour
        mi = date.minute
        s = date.second
        return f'new Date({y},{mo},{d},{h},{mi},{s})'

    # Do special processing needed to format predicates that come in
    # such as "gt(a)" correctly.
    def process_predicate(self, p):
        res = str(p).split('(')[0] + '('

        if type(p.value) == list:
            res += '['
            for v in p.value:
                res += self.fixup(v) + ','
            res = res[0:-1] + ']'
        else:
            res += self.fixup(p.value)
            if p.other is not None:
                res+= ',' + self.fixup(p.other)
        res += ')'
        return res

    # Special processing to handle strategies
    def process_strategy(self, s):
        c = 0
        res = f'new {str(s)}('
        for key in s.configuration:
            res += ',' if c > 0 else ''
            res += key + ':'
            val = s.configuration[key]
            if isinstance(val, Traversal):
                res += self.translate(val.bytecode, child=True)
            else:
                res += self.fixup(val)
            c += 1
        res += ')'
        return res
        pass

    # Main driver of the translation. Different parts of
    # a Traversal are handled appropriately.
    def do_translation(self, step):
        script = ''
        params = step[1:]
        script += '.' + step[0] + '('
        if len(params) > 0:
            c = 0
            with_opts = False
            for p in params:
                script += ',' if c > 0 else ''
                if with_opts:
                  script += f'WithOptions.{self.options[p]}'
                elif type(p) == Bytecode:
                    script += self.translate(p, True)
                elif type(p) == P:
                    script += self.process_predicate(p)
                elif type(p) in [Cardinality, Pop, Operator]:
                    tmp = str(p)
                    script += tmp[0:-1] if tmp.endswith('_') else tmp 
                elif type(p) in [ReadOnlyStrategy, SubgraphStrategy, VertexProgramStrategy,
                                 OptionsStrategy, PartitionStrategy]:
                    script += self.process_strategy(p)
                elif type(p) == datetime:
                    script += self.process_date(p)
                elif p == WithOptions.tokens:
                    script += 'WithOptions.tokens'
                    with_opts = True
                elif type(p) == str:
                    script += f'\'{p}\''
                elif type(p) == bool:
                    script += 'true' if p else 'false'
                else:
                    script += str(p)
                c += 1
        script += ')'
        return script

    # Translation starts here. There are two main parts to a 
    # traversal. Source instructions such as "withSideEffect"
    # and "withStrategies", and step instructions such as 
    # "addV" and "repeat". If child is True we will generate
    # anonymous traversal style syntax.
    def translate(self, bytecode, child=False):
        script = '__' if child else self.traversal_source
        
        for step in bytecode.source_instructions:
            script += self.do_translation(step)

        for step in bytecode.step_instructions:
            script += self.do_translation(step)
       
        return script
