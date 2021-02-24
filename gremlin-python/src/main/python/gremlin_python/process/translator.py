'''
Class that can turn traversals back into Gremlin Groovy format text queries.
Those queries can then be run in the Gremlin console or using the GLV submit(<String>) API or
sent to any TinkerPop compliant HTTP endpoint.


'''
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
    options = {
      WithOptions.tokens: 'tokens',
      WithOptions.none: 'none',
      WithOptions.ids:'ids',
      WithOptions.labels: 'labels',
      WithOptions.keys: 'keys',
      WithOptions.values: 'values',
      WithOptions.all: 'all',
      WithOptions.indexer: 'indexer',
      WithOptions.list: 'list',
      WithOptions.map: 'map'                           
    }

    def __init__(self,traversal_source=None):
        self.traversal_source = traversal_source

    def get_traversal_source(self):
        return self.traversal_source

    def get_target_language(self):
        return "gremlin-groovy"

    def of(self,traversal_source):
        self.traversal_source = traversal_source
        return self

    def fixup(self,v):
        if type(v) == str:
            return f'\'{v}\''
        elif type(v) == datetime:
            return self.process_date(v)
        else:
            return str(v)

    def process_date(selfself,date):
        y = date.year - 1900
        mo = date.month
        d = date.day
        h = date.hour
        mi = date.minute
        s = date.second
        return f'new Date({y},{mo},{d},{h},{mi},{s})'

    def process_predicate(self,p):
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

    def process_strategy(self,s):
        c = 0
        res = f'new {str(s)}('
        for key in s.configuration:
            res += ',' if c > 0 else ''
            res += key + ':'
            val = s.configuration[key]
            if isinstance(val,Traversal):
                res += self.translate(val.bytecode,child=True)
            else:
                res += self.fixup(val)
            c += 1
        res += ')'
        return res
        pass

    def do_translation(self,step):
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
                elif type(p) in [Cardinality,Pop]:
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
                else:
                    script += str(p)
                c += 1
        script += ')'
        return script

    def translate(self,bytecode,child=False):
        script = '__' if child else self.traversal_source
        
        for step in bytecode.source_instructions:
            #print(f'Source {step}')
            script += self.do_translation(step)

        for step in bytecode.step_instructions:
            #print(f'Step {step}')
            script += self.do_translation(step)
       
        return script
       
