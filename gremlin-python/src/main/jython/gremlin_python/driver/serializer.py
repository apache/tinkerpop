"""
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
"""
try:
    import ujson as json
except ImportError:
    import json

from gremlin_python.structure.io import graphson

__author__ = 'David M. Brown (davebshow@gmail.com)'


class Processor:
    """Base class for OpProcessor serialization system."""

    def __init__(self, writer):
        self._graphson_writer = writer

    def get_op_args(self, op, args):
        op_method = getattr(self, op, None)
        if not op_method:
            raise Exception("Processor does not support op: {}".format(op))
        return op_method(args)


class Standard(Processor):

    def authentication(self, args):
        return args

    def eval(self, args):
        return args


class Traversal(Processor):

    def authentication(self, args):
        return args

    def bytecode(self, args):
        gremlin = args['gremlin']
        args['gremlin'] = self._graphson_writer.toDict(gremlin)
        aliases = args.get('aliases', '')
        if not aliases:
            aliases = {'g': 'g'}
        args['aliases'] = aliases
        return args

    def close(self, args):
        return self.keys(args)

    def gather(self, args):
        side_effect = args['sideEffect']
        args['sideEffect'] = {'@type': 'g:UUID', '@value': side_effect}
        aliases = args.get('aliases', '')
        if not aliases:
            aliases = {'g': 'g'}
        args['aliases'] = aliases
        return args

    def keys(self, args):
        side_effect = args['sideEffect']
        args['sideEffect'] = {'@type': 'g:UUID', '@value': side_effect}
        return args


class GraphSONMessageSerializer:
    """Message serializer for GraphSON"""

    def __init__(self, reader=None, writer=None):
        if not reader:
            reader = graphson.GraphSONReader()
        self._graphson_reader = reader
        if not writer:
            writer = graphson.GraphSONWriter()
        self.standard = Standard(writer)
        self.traversal = Traversal(writer)

    def get_processor(self, processor):
        processor = getattr(self, processor, None)
        if not processor:
            raise Exception("Unknown processor")
        return processor

    def serialize_message(self, request_id, request_message):
        processor = request_message.processor
        op = request_message.op
        args = request_message.args
        if not processor:
            processor_obj = self.get_processor('standard')
        else:
            processor_obj = self.get_processor(processor)
        args = processor_obj.get_op_args(op, args)
        message = self.build_message(request_id, processor, op, args)
        return message

    def build_message(self, request_id, processor, op, args):
        message = {
            'requestId': {'@type': 'g:UUID', '@value': request_id},
            'processor': processor,
            'op': op,
            'args': args
        }
        return self.finalize_message(message, b"\x21",
                                     b"application/vnd.gremlin-v2.0+json")

    def finalize_message(self, message, mime_len, mime_type):
        message = json.dumps(message)
        message = b''.join([mime_len, mime_type, message.encode('utf-8')])
        return message

    def deserialize_message(self, message):
        return self._graphson_reader.toObject(message)
