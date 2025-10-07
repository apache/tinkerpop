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

@StepClassFilter @StepAggregate
Feature: Step - aggregate()

  Scenario: g_V_aggregateXxX_byXnameX_byXageX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("x").by("name").by("age").cap("x")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Aggregate step can only have one by modulator"

  Scenario: g_V_localXaggregateXxX_byXnameXX_byXageX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().local(aggregate("x").by("name").by("age")).cap("x")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Aggregate step can only have one by modulator"
