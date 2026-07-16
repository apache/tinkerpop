/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.jsr223;

import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.Optional;

/**
 * @author Valentyn Kahamlyk
 */
@State(Scope.Thread)
public class GremlinScriptCheckerBenchmark extends AbstractBenchmarkBase {

    @Benchmark
    public Optional<Long> testParseTimeout() {
        return GremlinScriptChecker.parse("g.with('timeoutMillis', 1000L).with(true).V().out('knows')").getTimeout();
    }

    @Benchmark
    public Optional<String> testParseMaterializeProperties() {
        return GremlinScriptChecker.parse("g.with('materializeProperties', 'all').with(true).V().out('knows')").getMaterializeProperties();
    }

    @Benchmark
    public GremlinScriptChecker.Result testParseAll() {
        return GremlinScriptChecker.parse("g.with('timeoutMillis', 1000L).with('materializeProperties', 'all').with('language', 'gremlin-lang').with('batchSize', 100).with('bulkResults', true).with(true).V().out('knows')");
    }
}
