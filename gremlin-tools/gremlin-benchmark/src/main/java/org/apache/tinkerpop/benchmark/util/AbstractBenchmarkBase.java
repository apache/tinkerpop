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

import org.junit.Test;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Base class for all TinkerPop OpenJDK JMH benchmarks.  Based upon Netty's approach to running JMH benchmarks
 * from JUnit.
 *
 * @see <a href="http://netty.io/wiki/microbenchmarks.html"</a>
 *
 * @author Ted Wilmes (http://twilmes.org)
 */
@Warmup(iterations = AbstractBenchmarkBase.DEFAULT_WARMUP_ITERATIONS)
@Measurement(iterations = AbstractBenchmarkBase.DEFAULT_MEASURE_ITERATIONS)
@Fork(AbstractBenchmarkBase.DEFAULT_FORKS)
public abstract class AbstractBenchmarkBase {

    protected static final int DEFAULT_WARMUP_ITERATIONS = 3;
    protected static final int DEFAULT_MEASURE_ITERATIONS = 10;
    protected static final int DEFAULT_FORKS = 2;
    protected static final String DEFAULT_BENCHMARK_DIRECTORY = "./benchmarks/";
    protected static final String DEFAULT_JVM_ARGS = "-server -Xms2g -Xmx2g";

    @Test
    public void run() throws Exception {
        final String className = getClass().getSimpleName();

        final ChainedOptionsBuilder runnerOptions = new OptionsBuilder()
                .include(".*" + className + ".*")
                .jvmArgs(getJvmArgs());

        if (getWarmupIterations() > 0) {
            runnerOptions.warmupIterations(getWarmupIterations());
        }

        if (getMeasureIterations() > 0) {
            runnerOptions.measurementIterations(getMeasureIterations());
        }

        if (getForks() > 0) {
            runnerOptions.forks(getForks());
        }

        if (getReportDir() != null) {
            final String dtmStr = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            final String filePath = getReportDir() + className + "-" + dtmStr + ".json";
            final File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            } else {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }

            runnerOptions.resultFormat(ResultFormatType.JSON);
            runnerOptions.result(filePath);
        }

        new Runner(runnerOptions.build()).run();
    }

    protected int getWarmupIterations() {
        return getIntProperty("warmupIterations", DEFAULT_WARMUP_ITERATIONS);
    }

    protected int getMeasureIterations() {
        return getIntProperty("measureIterations", DEFAULT_MEASURE_ITERATIONS);
    }

    protected int getForks() {
        return getIntProperty("forks", DEFAULT_FORKS);
    }

    protected String getReportDir() {
        return System.getProperty("benchmarkReportDir", DEFAULT_BENCHMARK_DIRECTORY);
    }

    protected String[] getJvmArgs() {
        return System.getProperty("jvmArgs", DEFAULT_JVM_ARGS).split(" ");
    }

    private int getIntProperty(final String propertyName, final int defaultValue) {
        final String propertyValue = System.getProperty(propertyName);
        if(propertyValue == null) {
            return defaultValue;
        }
        return Integer.valueOf(propertyValue);
    }
}
