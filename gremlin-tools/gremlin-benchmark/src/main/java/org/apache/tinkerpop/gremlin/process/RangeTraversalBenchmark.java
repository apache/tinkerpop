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
package org.apache.tinkerpop.gremlin.process;

import java.util.List;
import org.apache.tinkerpop.benchmark.util.AbstractGraphBenchmark;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.openjdk.jmh.annotations.Benchmark;

@LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
public class RangeTraversalBenchmark extends AbstractGraphBenchmark {

    @Override
    protected int getWarmupIterations() {
        return 1;
    }

    @Override
    protected int getForks() {
        return 1;
    }

    @Benchmark
    public List<Vertex> limit() {
        return g.V().hasLabel("artist").limit(10).toList();
    }

    @Benchmark
    public List<Vertex> range() {
        return g.V().hasLabel("artist").range(5, 20).toList();
    }

    @Benchmark
    public List<Vertex> skip() {
        return g.V().hasLabel("artist").skip(5).toList();
    }
}
