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

/**
 * TinkerGraph, the in-memory reference {@code Graph} implementation, and its {@code GraphComputer}. The same
 * descriptor is used by the regular jar and the jar with the {@code min} classifier, which differ only in the
 * bundled Gryo sample data. Every package is exported except the internal step packages
 * {@code process.traversal.step.map} and {@code process.traversal.step.sideEffect}.
 */
module org.apache.tinkerpop.gremlin.tinkergraph {
    requires transitive org.apache.tinkerpop.gremlin.core;
    requires org.apache.tinkerpop.gremlin.gql;
    requires org.apache.commons.lang3;

    exports org.apache.tinkerpop.gremlin.tinkergraph.jsr223;
    exports org.apache.tinkerpop.gremlin.tinkergraph.process.computer;
    exports org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization;
    exports org.apache.tinkerpop.gremlin.tinkergraph.services;
    exports org.apache.tinkerpop.gremlin.tinkergraph.structure;
    exports org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

    provides org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin
        with org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin;
    provides org.apache.tinkerpop.gremlin.structure.util.DefaultGraphProvider
        with org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphDefaultProvider;
}
