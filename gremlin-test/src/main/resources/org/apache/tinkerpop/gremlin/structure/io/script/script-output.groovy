import org.apache.tinkerpop.gremlin.structure.Direction

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
def stringify(vertex) {
    def edgeMap = { vdir ->
        return {
            def e = it.get()
            def g = e.graph().traversal(standard())
            g.E(e).values("weight").inject(e.label(), g.E(e).toV(Direction.valueOf(vdir.toUpperCase())).next().id()).join(":")
        }
    }
    def g = vertex.graph().traversal(standard())
    def v = g.V(vertex).values("name", "age", "lang").inject(vertex.id(), vertex.label()).join(":")
    def outE = g.V(vertex).outE().map(edgeMap("in")).join(",")
    def inE = g.V(vertex).inE().map(edgeMap("out")).join(",")
    return [v, outE, inE].join("\t")
}