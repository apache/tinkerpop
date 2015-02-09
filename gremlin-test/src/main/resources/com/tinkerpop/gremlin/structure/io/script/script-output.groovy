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
            e.values("weight").inject(e.label(), e."${vdir}V"().next().id()).join(":")
        }
    }
    def v = vertex.values("name", "age", "lang").inject(vertex.id(), vertex.label()).join(":")
    def outE = vertex.outE().map(edgeMap("in")).join(",")
    def inE = vertex.inE().map(edgeMap("out")).join(",")
    return [v, outE, inE].join("\t")
}