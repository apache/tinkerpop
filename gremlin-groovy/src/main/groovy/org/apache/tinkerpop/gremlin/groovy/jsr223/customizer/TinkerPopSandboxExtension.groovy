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
package org.apache.tinkerpop.gremlin.groovy.jsr223.customizer

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph

/**
 * This is an example class showing how one might use the {@link AbstractSandboxExtension}.  It uses a static method
 * white list and variable type mappings. It also prevents assigning non-typed variables the type of {@code Object}.
 * It is doubtful that this class would be useful for production applications and it simply serves as an example
 * on which users can extend and learn from.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class TinkerPopSandboxExtension extends AbstractSandboxExtension {

    private static final List<String> methodWhiteList = ["java\\.util\\..*",
                                                         "org\\.codehaus\\.groovy\\.runtime\\.DefaultGroovyMethods.*",
                                                         "org\\.apache\\.tinkerpop\\.gremlin\\.structure\\..*",
                                                         "org\\.apache\\.tinkerpop\\.gremlin\\.process\\..*",
                                                         "org\\.apache\\.tinkerpop\\.gremlin\\.process\\.traversal\\.dsl\\.graph\\..*"]
    private static final Map<String, Class<?>> staticVariableTypes = [graph:Graph, g: GraphTraversalSource]

    @Override
    List<String> getMethodWhiteList() {
        return methodWhiteList
    }

    @Override
    Map<String, Class<?>> getStaticVariableTypes() {
        return staticVariableTypes
    }

    @Override
    boolean allowAutoTypeOfUnknown() {
        return false
    }
}
