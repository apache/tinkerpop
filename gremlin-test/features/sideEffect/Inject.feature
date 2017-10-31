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

Feature: Step - inject()

  Scenario: g_VX1X_out_injectXv2X_name
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2 defined as "v[vadas]"
    And the traversal of
      """
      g.V(v1Id).out().inject(v2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | lop   |
      | vadas |
      | josh  |


  Scenario: g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter c defined as "c[it.get().length()]"
    And the traversal of
      """
      g.V(v1Id).out().values("name").inject("daniel").as("a").map(c).path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[daniel,d[6]] |
      | p[v[marko],v[lop],lop,d[3]] |
      | p[v[marko],v[vadas],vadas,d[5]] |
      | p[v[marko],v[josh],josh,d[4]] |
