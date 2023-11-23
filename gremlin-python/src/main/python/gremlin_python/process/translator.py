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

import math
import numbers
import re

from gremlin_python.process.traversal import *
from gremlin_python.process.strategies import *
from gremlin_python.structure.graph import Vertex, Edge, VertexProperty
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

    conn_p = ['and', 'or']

    def __init__(self, traversal_source=None):
        self.traversal_source = traversal_source

    def get_traversal_source(self):
        return self.traversal_source

    def get_target_language(self):
        return "gremlin-groovy"

    def of(self, traversal_source):
        self.traversal_source = traversal_source
        return self

    # Do any needed special processing for the representation
    # of strings and dates and boolean.
    def fixup(self, v):
        if isinstance(v, str):
            return f'{v!r}'  # use repr() format for canonical string rep
        elif type(v) == datetime:
            return self.process_date(v)
        elif type(v) == bool:
            return 'true' if v else 'false'
        elif isinstance(v, numbers.Number):
            return self.process_number(v)
        elif isinstance(v, set):
            return f'[{str(v)[1:-1]}]'
        elif isinstance(v, P):
            return self.process_predicate(v)
        elif type(v) == Vertex:
            return self.process_vertex(v)
        elif type(v) == Edge:
            return self.process_edge(v)
        elif type(v) in [Merge]:  # on_create on_match out_v in_v
            tmp = str(v)
            return f'{tmp.split("_")[0]}{tmp.split("_")[1].capitalize()}' if tmp.find('_') else tmp
        elif v is None:
            return 'null'
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
        res = ''
        if p.operator in self.conn_p:
            res += f'{self.process_predicate(p.value)}.{p.operator}({self.process_predicate(p.other)})'
        else:
            res += f'{self.process_p_value(p)}'
        return res

    # process the value of the predicates
    def process_p_value(self, p):
        res = str(p).split('(')[0] + '('
        if type(p.value) == list:
            res += '['
            for v in p.value:
                res += self.fixup(v) + ','
            res = (res[0:-1] + ']') if len(p.value) > 0 else (res + ']')
        else:
            res += self.fixup(p.value)
            if p.other is not None:
                res += f',{self.fixup(p.other)}'
        res += ')'
        return res

    # Special processing to handle strategies
    def process_strategy(self, s):
        c = 0
        res = ''
        # if parameter is empty, only pass class name (referenced GroovyTranslator.java)
        if not s.configuration:
            res += s.strategy_name
        else:
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

    # Special processing to handle vertices
    def process_vertex(self, vertex):
        return f'new ReferenceVertex({self.fixup(vertex.id)},\'{vertex.label}\')'

    # Special processing to handle edges
    def process_edge(self, edge):
        return f'new ReferenceEdge({str(edge.id)},\'{edge.label}\',' \
               f'new ReferenceVertex({str(edge.inV.id)},\'{edge.inV.label}\'),' \
               f'new ReferenceVertex({str(edge.outV.id)},\'{edge.outV.label}\'))'

    # Special processing to handle vertex property
    def process_vertex_property(self, vp):
        return f'new ReferenceVertexProperty({str(vp.id)},\'{vp.label}\',{self.fixup(vp.value)})'

    # Special processing to handle lambda
    def process_lambda(self, lam):
        lambda_result = lam()
        script = lambda_result if isinstance(lambda_result, str) else lambda_result[0]
        return f'{script}' if re.match(r"^\{.*\}$", script, flags=re.DOTALL) else f'{{{script}}}'

    def process_dict(self, d):
        c = 0
        res = '['
        if len(d) == 0:
            res += ':'
        else:
            for k, v in d.items():
                wrap = not isinstance(k, str)
                res += ',' if c > 0 else ''
                res += '(' if wrap else ''
                res += self.fixup(k)
                res += ')' if wrap else ''
                res += f':{self.fixup(v)}'
                c += 1
        res += ']'
        return res

    def process_number(self, n):
        if isinstance(n, float):
            # converting floats into doubles for script since python doesn't distinguish and java defaults to double
            if math.isnan(n):
                return "Double.NaN"
            elif math.isinf(n) and n > 0:
                return "Double.POSITIVE_INFINITY"
            elif math.isinf(n) and n < 0:
                return "Double.NEGATIVE_INFINITY"
            else:
                return f'{n}d'
        return f'{n}'

    def process_list(self, l):
        c = 0
        res = '['
        for i in l:
            res += ',' if c > 0 else ''
            res += self.fixup(i)
            c += 1
        res += ']'
        return res

    # Special processing to handle bindings inside of traversals
    def process_binding(self, binding):
        return f'Bindings.instance().of(\'{binding.key}\', {str(binding.value)})'

    # Main driver of the translation. Different parts of
    # a Traversal are handled appropriately.
    def do_translation(self, step):
        script = ''
        params = step[1:]
        script += '.' + step[0] + '('
        if len(params) > 0:
            c = 0
            with_opts = False
            is_merge_op = (step[0] == 'mergeV') or (step[0] == 'mergeE')
            for p in params:
                script += ',' if c > 0 else ''
                if with_opts:
                    script += f'WithOptions.{self.options[p]}'
                elif type(p) == Bytecode:
                    script += self.translate(p, True)
                elif isinstance(p, P):
                    script += self.process_predicate(p)
                elif type(p) == Vertex:
                    script += self.process_vertex(p)
                elif type(p) == Edge:
                    script += self.process_edge(p)
                elif type(p) == VertexProperty:
                    script += self.process_vertex_property(p)
                elif type(p) in [Cardinality, Pop, Operator, Scope, T]:
                    tmp = str(p)
                    script += tmp[0:-1] if tmp.endswith('_') else tmp
                elif type(p) in [Merge]:  # on_create on_match out_v in_v
                    is_merge_op = True
                    tmp = str(p)
                    script += f'{tmp.split("_")[0]}{tmp.split("_")[1].capitalize()}' if tmp.find('_') else tmp
                elif isinstance(p, TraversalStrategy):  # this will capture all strategies
                    script += self.process_strategy(p)
                elif type(p) == datetime:
                    script += self.process_date(p)
                elif p == WithOptions.tokens:
                    script += 'WithOptions.tokens'
                    with_opts = True
                elif isinstance(p, str):
                    script += f'{p!r}'  # use repr() format for canonical string rep
                elif type(p) == bool:
                    script += 'true' if p else 'false'
                elif isinstance(p, type(lambda: None)) and p.__name__ == (lambda: None).__name__:
                    script += self.process_lambda(p)
                elif type(p) == Binding:
                    script += self.process_binding(p)
                elif p is None:
                    script += '(Traversal) null' if is_merge_op else 'null'
                elif isinstance(p, type):
                    script += p.__name__
                elif isinstance(p, dict):
                    script += self.process_dict(p)
                elif isinstance(p, numbers.Number):
                    script += self.process_number(p)
                elif isinstance(p, set):
                    script += f'[{str(p)[1:-1]}] as Set' if len(p) > 0 else '[] as Set'
                elif isinstance(p, list):
                    script += self.process_list(p)
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
