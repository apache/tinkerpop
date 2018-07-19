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

Feature: Step - read()

  Scenario: g_io_readXkryoX
    Given the empty graph
    And the traversal of
      """
      g.io("data/tinkerpop-modern.kryo").read()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 6 for count of "g.V()"
    And the graph should return 6 for count of "g.E()"

  Scenario: g_io_read_withXreader_gryoX
    Given the empty graph
    And the traversal of
      """
      g.io("data/tinkerpop-modern.kryo").with(IO.reader, IO.gryo).read()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 6 for count of "g.V()"
    And the graph should return 6 for count of "g.E()"

  Scenario: g_io_readXgraphsonX
    Given the empty graph
    And the traversal of
      """
      g.io("data/tinkerpop-modern.json").read()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 6 for count of "g.V()"
    And the graph should return 6 for count of "g.E()"

  Scenario: g_io_read_withXreader_graphsonX
    Given the empty graph
    And the traversal of
      """
      g.io("data/tinkerpop-modern.json").with(IO.reader, IO.graphson).read()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 6 for count of "g.V()"
    And the graph should return 6 for count of "g.E()"

  Scenario: g_io_readXgraphmlX
    Given the empty graph
    And the traversal of
      """
      g.io("data/tinkerpop-modern.xml").read()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 6 for count of "g.V()"
    And the graph should return 6 for count of "g.E()"

  Scenario: g_io_read_withXreader_graphmlX
    Given the empty graph
    And the traversal of
      """
      g.io("data/tinkerpop-modern.xml").with(IO.reader, IO.graphml).read()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 6 for count of "g.V()"
    And the graph should return 6 for count of "g.E()"