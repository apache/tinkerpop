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
package org.apache.tinkerpop.gremlin.spark.structure.io;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Boxuan Li (https://li-boxuan.com)
 */
public class SparkRDDLoadTest extends AbstractSparkTest {
    @Test
    public void shouldLoadVerticesRDDWithInputRDD() {
        final JavaSparkContext sparkContext = getSparkContext();

        // load vertices with inputRDD config
        final Configuration sparkGraphConfiguration = new BaseConfiguration();
        sparkGraphConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, ExampleInputRDD.class.getCanonicalName());
        JavaPairRDD<Object, VertexWritable> verticesRDD = SparkIOUtil.loadVertices(sparkGraphConfiguration, sparkContext);
        assertEquals(4, verticesRDD.count());
        assertEquals(123, verticesRDD.values()
            .map(vertexWritable -> (int) vertexWritable.get().values("age").next())
            .reduce(Integer::sum).longValue());

        // load vertices with inputRDD object
        verticesRDD = SparkIOUtil.loadVertices(new ExampleInputRDD(), new BaseConfiguration(), sparkContext);
        assertEquals(4, verticesRDD.count());
        assertEquals(123, verticesRDD.values()
            .map(vertexWritable -> (int) vertexWritable.get().values("age").next())
            .reduce(Integer::sum).longValue());

        sparkContext.stop();
    }

    @Test
    public void shouldLoadVerticesRDDWithInputFormat() {
        final JavaSparkContext sparkContext = getSparkContext();

        final Configuration sparkGraphConfiguration = new BaseConfiguration();
        sparkGraphConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        sparkGraphConfiguration.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR,
                TestFiles.PATHS.get("tinkerpop-modern-v3.kryo"));

        // load vertices
        JavaPairRDD<Object, VertexWritable> verticesRDD = SparkIOUtil.loadVertices(sparkGraphConfiguration, sparkContext);
        assertEquals(6, verticesRDD.count());

        sparkContext.stop();
    }

    private JavaSparkContext getSparkContext() {
        SparkConf sparkConf = new SparkConf()
            .setAppName("Spark Graph")
            .set(SparkLauncher.SPARK_MASTER, "local[4]")
            .set(Constants.SPARK_SERIALIZER, GryoSerializer.class.getCanonicalName());
        return new JavaSparkContext(sparkConf);
    }
}
