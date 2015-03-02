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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkGraphComputer implements GraphComputer {

    public static final Logger LOGGER = LoggerFactory.getLogger(SparkGraphComputer.class);

    protected final SparkConf configuration = new SparkConf();

    protected final HadoopGraph hadoopGraph;

    private boolean executed = false;
    private final Set<MapReduce> mapReduces = new HashSet<>();
    private VertexProgram vertexProgram;

    public SparkGraphComputer(final HadoopGraph hadoopGraph) {
        this.hadoopGraph = hadoopGraph;
    }

    public static void main(final String[] args) {
        final SparkConf configuration = new SparkConf();
        configuration.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX);
        configuration.setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(configuration);
        JavaRDD<String> rdd = sc.textFile("README.asciidoc");
        System.out.println("You made it: " + rdd.count());
    }


    @Override
    public GraphComputer isolation(final Isolation isolation) {
        if (!isolation.equals(Isolation.BSP))
            throw GraphComputer.Exceptions.isolationNotSupported(isolation);
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReduces.add(mapReduce);
        return this;
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    @Override
    public Future<ComputerResult> submit() {
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;

        // it is not possible execute a computer if it has no vertex program nor mapreducers
        if (null == this.vertexProgram && this.mapReduces.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram)
            GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);

        final long startTime = System.currentTimeMillis();
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            return null;
        });
    }

}
