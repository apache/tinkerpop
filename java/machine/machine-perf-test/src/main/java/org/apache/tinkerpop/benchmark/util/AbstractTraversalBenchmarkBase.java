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
package org.apache.tinkerpop.benchmark.util;

import org.apache.tinkerpop.language.gremlin.common.__;
import org.openjdk.jmh.annotations.Benchmark;

import java.util.List;

public class AbstractTraversalBenchmarkBase extends AbstractProcessorBenchmark {

    @Benchmark
    public Object g_inject_unfold_incr_incr_incr_incr() {
        return g.inject(input).unfold().incr().incr().incr().incr().toList();
    }

    @Benchmark
    public List g_inject_unfold_repeat_times() {
        return g.inject(input).unfold().repeat(__.incr()).times(4).toList();
    }
}
