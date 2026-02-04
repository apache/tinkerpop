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

@StepClassSideEffect @StepWrite
Feature: Step - write()

  Scenario: g_io_writeXkryoX
    Given the modern graph
    And the traversal of
      """
      g.io("tinkerpop-modern-v3.kryo").write()
      """
    When iterated next
    Then the file "tinkerpop-modern-v3.kryo" should exist

  Scenario: g_io_write_withXwriter_gryoX
    Given the modern graph
    And the traversal of
      """
      g.io("tinkerpop-modern-v3.kryo").with(IO.writer, IO.gryo).write()
      """
    When iterated next
    Then the file "tinkerpop-modern-v3.kryo" should exist

  Scenario: g_io_writeXgraphsonX
    Given the modern graph
    And the traversal of
      """
      g.io("tinkerpop-modern-v3.json").write()
      """
    When iterated next
    Then the file "tinkerpop-modern-v3.json" should exist

  Scenario: g_io_write_withXwriter_graphsonX
    Given the modern graph
    And the traversal of
      """
      g.io("tinkerpop-modern-v3.json").with(IO.writer, IO.graphson).write()
      """
    When iterated next
    Then the file "tinkerpop-modern-v3.json" should exist

  Scenario: g_io_writeXgraphmlX
    Given the modern graph
    And the traversal of
      """
      g.io("tinkerpop-modern.xml").write()
      """
    When iterated next
    Then the file "tinkerpop-modern.xml" should exist

  Scenario: g_io_write_withXwriter_graphmlX
    Given the modern graph
    And the traversal of
      """
      g.io("tinkerpop-modern.xml").with(IO.writer, IO.graphml).write()
      """
    When iterated next
    Then the file "tinkerpop-modern.xml" should exist