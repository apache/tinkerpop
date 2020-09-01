.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..  http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

=================================
Apache TinkerPop - Gremlin Python
=================================

`Apache TinkerPop™ <https://tinkerpop.apache.org>`_
is a graph computing framework for both graph databases (OLTP) and
graph analytic systems (OLAP). `Gremlin <https://tinkerpop.apache.org/gremlin.html>`_
is the graph traversal language of
TinkerPop. It can be described as a functional, data-flow language that enables users to succinctly express complex
traversals on (or queries of) their application's property graph.

Gremlin-Python implements Gremlin within the Python language and can be used on any Python virtual machine including
the popular CPython machine. Python’s syntax has the same constructs as Java including "dot notation" for function
chaining ``(a.b.c)``, round bracket function arguments ``(a(b,c))``, and support for global namespaces
``(a(b()) vs a(__.b()))``. As such, anyone familiar with Gremlin-Java will immediately be able to work with
Gremlin-Python. Moreover, there are a few added constructs to Gremlin-Python that make traversals a bit more succinct.

Gremlin-Python is designed to connect to a "server" that is hosting a TinkerPop-enabled graph system. That "server"
could be `Gremlin Server <https://tinkerpop.apache.org/docs/current/reference/#gremlin-server>`_ or a
`remote Gremlin provider <https://tinkerpop.apache.org/docs/current/reference/#connecting-rgp>`_ that exposes
protocols by which Gremlin-Python can connect.

A typical connection to a server running on "localhost" that supports the Gremlin Server protocol using websockets
from the Python shell looks like this:

    >>> from gremlin_python.process.anonymous_traversal import traversal
    >>> from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
    >>> g = traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin','g'))

Once "g" has been created using a connection, it is then possible to start writing Gremlin traversals to query the
remote graph:

    >>> g.V().both()[1:3].toList()
    [v[2], v[4]]
    >>> g.V().both()[1].toList()
    [v[2]]
    >>> g.V().both().name.toList()
    [lop, vadas, josh, marko, marko, josh, peter, ripple, lop, marko, josh, lop]

-----------------
Sample Traversals
-----------------

The Gremlin language allows users to write highly expressive graph traversals and has a broad list of functions that
cover a wide body of features. The `Reference Documentation <https://tinkerpop.apache.org/docs/current/reference/#graph-traversal-steps>`_
describes these functions and other aspects of the TinkerPop ecosystem including some specifics on
`Gremlin in Python <https://tinkerpop.apache.org/docs/current/reference/#gremlin-python>`_ itself. Most of the
examples found in the documentation use Groovy language syntax in the
`Gremlin Console <https://tinkerpop.apache.org/docs/current/tutorials/the-gremlin-console/>`_.
For the most part, these examples should generally translate to Python with
`some modification <https://tinkerpop.apache.org/docs/current/reference/#gremlin-python-differences>`_. Given the
strong correspondence between canonical Gremlin in Java and its variants like Python, there is a limited amount of
Python-specific documentation and examples. This strong correspondence among variants ensures that the general
Gremlin reference documentation is applicable to all variants and that users moving between development languages can
easily adopt the Gremlin variant for that language.

Create Vertex
^^^^^^^^^^^^^

.. code:: python

    from gremlin_python.process.traversal import T
    from gremlin_python.process.traversal import Cardinality

    id = T.id
    single = Cardinality.single

    def create_vertex(self, vid, vlabel):
        # default database cardinality is used when Cardinality argument is not specified
        g.addV(vlabel).property(id, vid). \
          property(single, 'name', 'Apache'). \
          property('lastname', 'Tinkerpop'). \
          next()

Find Vertices
^^^^^^^^^^^^^

.. code:: python

    def list_all(self, limit=500):
        g.V().limit(limit).elementMap().toList()

    def find_vertex(self, vid):
        g.V(vid).elementMap().next()

    def list_by_label_name(self, vlabel, name):
        g.V().has(vlabel, 'name', name).elementMap().toList()

Update Vertex
^^^^^^^^^^^^^

.. code:: python

    from gremlin_python.process.traversal import Cardinality

    single = Cardinality.single

    def update_vertex(self, vid, name):
        g.V(vid).property(single, 'name', name).next()

NOTE that versions suffixed with "rc" are considered release candidates (i.e. pre-alpha, alpha, beta, etc.) and
thus for early testing purposes only.