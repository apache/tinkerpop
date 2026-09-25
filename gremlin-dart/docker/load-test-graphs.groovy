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

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory

TinkerFactory.generateModern(modern)
TinkerFactory.generateClassic(classic)
TinkerFactory.generateTheCrew(crew)
TinkerFactory.generateGratefulDead(grateful)
TinkerFactory.generateKitchenSink(sink)

def globals = [:]
globals << [g       : traversal().withEmbedded(graph)]
globals << [ggraph  : traversal().withEmbedded(graph)]
globals << [gmodern : traversal().withEmbedded(modern)]
globals << [gclassic: traversal().withEmbedded(classic)]
globals << [gcrew   : traversal().withEmbedded(crew)]
globals << [ggrateful: traversal().withEmbedded(grateful)]
globals << [gsink   : traversal().withEmbedded(sink)]
