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
import pytest

from gremlin_python.process.traversal import GremlinLang, P
from gremlin_python.process.graph_traversal import (
    GraphTraversalSource, GraphTraversal)
from gremlin_python.process.graph_traversal import __ as AnonymousTraversal
from gremlin_python.process.anonymous_traversal import traversal

__author__ = 'David M. Brown (davebshow@gmail.com)'


class SocialTraversal(GraphTraversal):

    def knows(self, person_name):
        return self.out("knows").has_label("person").has("name", person_name)

    def youngestFriendsAge(self):
        return self.out("knows").has_label("person").values("age").min_()

    def createdAtLeast(self, number):
        return self.out_e("created").count().is_(P.gte(number))

class __(AnonymousTraversal):

    graph_traversal = SocialTraversal

    @classmethod
    def knows(cls, *args):
        return cls.graph_traversal(None, None, GremlinLang()).knows(*args)

    @classmethod
    def youngestFriendsAge(cls, *args):
        return cls.graph_traversal(None, None, GremlinLang()).youngestFriendsAge(*args)

    @classmethod
    def createdAtLeast(cls, *args):
        return cls.graph_traversal(None, None, GremlinLang()).createdAtLeast(*args)


class SocialTraversalSource(GraphTraversalSource):

    def __init__(self, *args, **kwargs):
        super(SocialTraversalSource, self).__init__(*args, **kwargs)
        self.graph_traversal = SocialTraversal

    def persons(self, *args):
        traversal = self.get_graph_traversal().V().has_label("person")

        if len(args) > 0:
            traversal = traversal.has("name", P.within(*args))

        return traversal


def test_dsl(remote_connection):
    social = traversal(SocialTraversalSource).with_(remote_connection)
    assert social.persons("marko").knows("josh").next()
    assert social.persons("marko").youngestFriendsAge().next() == 27
    assert social.persons().count().next() == 4
    assert social.persons("marko", "josh").count().next() == 2
    assert social.persons().filter_(__.createdAtLeast(2)).count().next() == 1
