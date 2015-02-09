/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
// Generates a graph into the "empty" graph using the graph generators. "g" refers to the empty graph configured in the standard yaml configuration
(0..<10000).each { g.addVertex("oid", it) }
DistributionGenerator.build(g).label("knows").seedGenerator {
    987654321l
}.outDistribution(new PowerLawDistribution(2.1)).inDistribution(new PowerLawDistribution(2.1)).expectedNumEdges(100000).create().generate();
