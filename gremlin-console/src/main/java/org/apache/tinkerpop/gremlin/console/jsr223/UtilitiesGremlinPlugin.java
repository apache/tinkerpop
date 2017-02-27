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
package org.apache.tinkerpop.gremlin.console.jsr223;

import groovyx.gprof.ProfileStaticExtension;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultScriptCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class UtilitiesGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.utilities";

    private static final ImportCustomizer imports;

    private static final ScriptCustomizer scripts;

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(groovyx.gbench.Benchmark.class,
                            groovyx.gbench.BenchmarkBuilder.class,
                            groovyx.gbench.BenchmarkConstants.class,
                            groovyx.gbench.BenchmarkContext.class,
                            groovyx.gbench.Benchmarker.class,
                            groovyx.gbench.BenchmarkList.class,
                            groovyx.gbench.BenchmarkLogger.class,
                            groovyx.gbench.BenchmarkMath.class,
                            groovyx.gbench.BenchmarkMeasure.class,
                            groovyx.gbench.BenchmarkStaticExtension.class,
                            groovyx.gbench.BenchmarkSystem.class,
                            groovyx.gbench.BenchmarkTime.class,
                            groovyx.gbench.BenchmarkWarmUp.class,
                            groovyx.gprof.Profiler.class,
                            groovyx.gprof.ProfileStaticExtension.class,
                            groovyx.gprof.CallFilter.class,
                            groovyx.gprof.CallInfo.class,
                            groovyx.gprof.CallInterceptor.class,
                            groovyx.gprof.CallMatcher.class,
                            groovyx.gprof.CallTree.class,
                            groovyx.gprof.MethodCallFilter.class,
                            groovyx.gprof.MethodCallInfo.class,
                            groovyx.gprof.MethodInfo.class,
                            groovyx.gprof.ProfileMetaClass.class,
                            groovyx.gprof.ProxyReport.class,
                            groovyx.gprof.Report.class,
                            groovyx.gprof.ReportElement.class,
                            groovyx.gprof.ReportNormalizer.class,
                            groovyx.gprof.ReportPrinter.class,
                            groovyx.gprof.ThreadInfo.class,
                            groovyx.gprof.ThreadRunFilter.class,
                            groovyx.gprof.Utils.class)
                    .addMethodImports(
                            ProfileStaticExtension.class.getMethod("profile", Object.class, Callable.class),
                            ProfileStaticExtension.class.getMethod("profile", Object.class, Map.class, Callable.class)).create();

            final BufferedReader reader = new BufferedReader(new InputStreamReader(UtilitiesGremlinPlugin.class.getResourceAsStream("UtilitiesGremlinPluginScript.groovy")));
            final List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            reader.close();

            scripts = new DefaultScriptCustomizer(Collections.singletonList(lines));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public UtilitiesGremlinPlugin() {
        super(NAME, imports, scripts);
    }
}
