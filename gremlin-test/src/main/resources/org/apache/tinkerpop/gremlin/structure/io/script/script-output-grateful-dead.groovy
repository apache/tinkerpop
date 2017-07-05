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

def stringify(v) {
    def vertex = [v.id(), v.label()]
    switch (v.label()) {
        case "song":
            vertex << v.value("name")
            vertex << v.value("songType")
            vertex << v.value("performances")
            break
        case "artist":
            vertex << v.value("name")
            break
        default:
            throw new Exception("Unexpected vertex label: ${v.label()}")
    }
    def g = v.graph().traversal()
    def outE = g.V(v).outE().map {
        def e = it.get()
        def weight = e.property("weight")
        def edge = [e.label(), e.inVertex().id()]
        if (weight.isPresent()) edge << weight.value()
        return edge.join(",")
    }
    def inE = g.V(v).inE().map {
        def e = it.get()
        def weight = e.property("weight")
        def edge = [e.label(), e.outVertex().id()]
        if (weight.isPresent()) edge << weight.value()
        return edge.join(",")
    }
    return [vertex.join(","), outE.join("|"), inE.join("|")].join("\t")
}