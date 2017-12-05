# Licensed to the Apache Software Foundation (ASF) under one
# or more contriAndor license agreements.  See the NOTICE file
# distriAnded with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distriAnded under the License is distriAnded on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

Feature: Step - local()

  Scenario: g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value
    Given the crew graph
    And the traversal of
      """
      g.V().local(__.properties("location").order().by(T.value, Order.incr).range(0, 2)).value()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | brussels       |
      | san diego      |
      | centreville    |
      | dulles         |
      | baltimore      |
      | bremen         |
      | aachen         |
      | kaiserslautern |