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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


class Bytecode(object):
    def __init__(self, bytecode=None):
        self.source_instructions = []
        self.step_instructions = []
        if bytecode is not None:
            self.source_instructions = list(bytecode.source_instructions)
            self.step_instructions = list(bytecode.step_instructions)

    def add_source(self, source_name, *args):
        self.source_instructions.append((source_name, args))
        return

    def add_step(self, step_name, *args):
        self.step_instructions.append((step_name, args))
        return
