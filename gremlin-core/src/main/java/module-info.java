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
 * The Gremlin traversal machine, structure API, IO and the {@code gremlin-lang} script engine. Every package is
 * exported except three that are entirely internal: {@code jsr223.console},
 * {@code process.computer.traversal.lambda} and {@code process.traversal.strategy.util}.
 * <p/>
 * javatuples has no stable module name and is required as an automatic module under its filename-derived name.
 */
module org.apache.tinkerpop.gremlin.core {
    requires transitive org.apache.tinkerpop.gremlin.language;
    // GraphSONMapper, GryoMapper and the IO registries expose shaded Jackson and Kryo types
    requires transitive org.apache.tinkerpop.gremlin.shaded;
    requires transitive org.apache.commons.configuration2;
    requires transitive org.apache.commons.collections4;
    requires transitive org.slf4j;
    requires transitive javatuples;
    // GremlinLangCustomizer takes and returns a Caffeine cache builder
    requires transitive com.github.benmanes.caffeine;
    // GremlinScriptEngine extends ScriptEngine
    requires transitive java.scripting;
    // GraphMLReader.Builder accepts an XMLInputFactory
    requires transitive java.xml;

    // commons-configuration2 only declares these as static, but GraphFactory needs them at runtime to load
    // file and YAML configurations
    requires org.apache.commons.beanutils;
    requires org.yaml.snakeyaml;

    requires org.apache.commons.lang3;
    requires org.apache.commons.text;
    requires exp4j;
    requires org.locationtech.jts;
    requires java.sql;

    exports org.apache.tinkerpop.core.annotation;
    exports org.apache.tinkerpop.gremlin.jsr223;
    exports org.apache.tinkerpop.gremlin.language.grammar;
    exports org.apache.tinkerpop.gremlin.language.translator;
    exports org.apache.tinkerpop.gremlin.process.computer;
    exports org.apache.tinkerpop.gremlin.process.computer.clone;
    exports org.apache.tinkerpop.gremlin.process.computer.clustering.connected;
    exports org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure;
    exports org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank;
    exports org.apache.tinkerpop.gremlin.process.computer.search.path;
    exports org.apache.tinkerpop.gremlin.process.computer.traversal;
    exports org.apache.tinkerpop.gremlin.process.computer.traversal.step;
    exports org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;
    exports org.apache.tinkerpop.gremlin.process.computer.traversal.strategy;
    exports org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration;
    exports org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.finalization;
    exports org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization;
    exports org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification;
    exports org.apache.tinkerpop.gremlin.process.computer.util;
    exports org.apache.tinkerpop.gremlin.process.remote;
    exports org.apache.tinkerpop.gremlin.process.remote.traversal;
    exports org.apache.tinkerpop.gremlin.process.remote.traversal.step.map;
    exports org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration;
    exports org.apache.tinkerpop.gremlin.process.traversal;
    exports org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;
    exports org.apache.tinkerpop.gremlin.process.traversal.lambda;
    exports org.apache.tinkerpop.gremlin.process.traversal.step;
    exports org.apache.tinkerpop.gremlin.process.traversal.step.branch;
    exports org.apache.tinkerpop.gremlin.process.traversal.step.filter;
    exports org.apache.tinkerpop.gremlin.process.traversal.step.map;
    exports org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;
    exports org.apache.tinkerpop.gremlin.process.traversal.step.util;
    exports org.apache.tinkerpop.gremlin.process.traversal.step.util.event;
    exports org.apache.tinkerpop.gremlin.process.traversal.strategy;
    exports org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;
    exports org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;
    exports org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;
    exports org.apache.tinkerpop.gremlin.process.traversal.strategy.provider;
    exports org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;
    exports org.apache.tinkerpop.gremlin.process.traversal.traverser;
    exports org.apache.tinkerpop.gremlin.process.traversal.traverser.util;
    exports org.apache.tinkerpop.gremlin.process.traversal.util;
    exports org.apache.tinkerpop.gremlin.structure;
    exports org.apache.tinkerpop.gremlin.structure.geo;
    exports org.apache.tinkerpop.gremlin.structure.io;
    exports org.apache.tinkerpop.gremlin.structure.io.binary;
    exports org.apache.tinkerpop.gremlin.structure.io.binary.types;
    exports org.apache.tinkerpop.gremlin.structure.io.graphml;
    exports org.apache.tinkerpop.gremlin.structure.io.graphson;
    exports org.apache.tinkerpop.gremlin.structure.io.gryo;
    exports org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim;
    exports org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded;
    exports org.apache.tinkerpop.gremlin.structure.io.pdt;
    exports org.apache.tinkerpop.gremlin.structure.io.util;
    exports org.apache.tinkerpop.gremlin.structure.service;
    exports org.apache.tinkerpop.gremlin.structure.util;
    exports org.apache.tinkerpop.gremlin.structure.util.detached;
    exports org.apache.tinkerpop.gremlin.structure.util.empty;
    exports org.apache.tinkerpop.gremlin.structure.util.keyed;
    exports org.apache.tinkerpop.gremlin.structure.util.reference;
    exports org.apache.tinkerpop.gremlin.structure.util.star;
    exports org.apache.tinkerpop.gremlin.structure.util.wrapped;
    exports org.apache.tinkerpop.gremlin.util;
    exports org.apache.tinkerpop.gremlin.util.function;
    exports org.apache.tinkerpop.gremlin.util.iterator;

    uses org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
    uses org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimService;
    uses org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeAdapter;
    uses org.apache.tinkerpop.gremlin.structure.util.DefaultGraphProvider;

    provides org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory
        with org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngineFactory;
}
