import org.apache.tinkerpop.gremlin.structure.VertexProperty

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
def parse(line, factory) {
    def parts = line.split(/\t/)
    def (id, label, name, x) = parts[0].split(/:/).toList()
    def v1 = factory.vertex(id, label)
    if (name != null) v1.property(VertexProperty.Cardinality.single, "name", name)
    if (x != null) {
        if (label.equals("project")) v1.property(VertexProperty.Cardinality.single, "lang", x)
        else v1.property(VertexProperty.Cardinality.single, "age", Integer.valueOf(x))
    }
    // process out-edges
    if (parts.length >= 2) {
        parts[1].split(/,/).grep { !it.isEmpty() }.each {
            def (eLabel, refId, weight) = it.split(/:/).toList()
            def v2 = factory.vertex(refId)
            def edge = factory.edge(v1, v2, eLabel)
            edge.property("weight", Double.valueOf(weight))
        }
    }
    // process in-edges
    if (parts.length == 3) {
        parts[2].split(/,/).grep { !it.isEmpty() }.each {
            def (eLabel, refId, weight) = it.split(/:/).toList()
            def v2 = factory.vertex(refId)
            def edge = factory.edge(v2, v1, eLabel)
            edge.property("weight", Double.valueOf(weight))
        }
    }
    return v1
}