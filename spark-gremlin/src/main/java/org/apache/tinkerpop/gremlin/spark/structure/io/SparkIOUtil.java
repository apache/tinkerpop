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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to load graph as Spark RDDs
 *
 * @author Boxuan Li (https://li-boxuan.com)
 */
public class SparkIOUtil {
    private static final Logger logger = LoggerFactory.getLogger(SparkIOUtil.class);

    /**
     * Load graph into Spark RDDs. Note: Graph configurations must include {@value Constants#GREMLIN_HADOOP_GRAPH_READER}.
     * <p>
     * See Example below:
     * <pre>
     *     SparkConf sparkConf = new SparkConf().setAppName("Spark Graph")
     *         .set(SparkLauncher.SPARK_MASTER, "local[4]")
     *         .set(Constants.SPARK_SERIALIZER, GryoSerializer.class.getCanonicalName());
     *     JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
     *
     *     Configuration sparkGraphConfiguration = new BaseConfiguration();
     *     sparkGraphConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
     *     sparkGraphConfiguration.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR,
     *         SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3.kryo"));
     *
     *     // load vertices
     *     JavaPairRDD verticesRDD = SparkIOUtil.loadVertices(sparkGraphConfiguration, sparkContext);
     * </pre>
     *
     * @param sparkGraphConfiguration graph configurations
     * @param sparkContext            a JavaSparkContext instance
     * @return vertices in Spark RDD
     */
    public static JavaPairRDD<Object, VertexWritable> loadVertices(
        final org.apache.commons.configuration2.Configuration sparkGraphConfiguration,
        final JavaSparkContext sparkContext) {
        assert sparkGraphConfiguration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_READER);
        logger.debug("Loading vertices into Spark RDD...");
        final Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(sparkGraphConfiguration);
        return loadVertices(createInputRDD(hadoopConfiguration), sparkGraphConfiguration, sparkContext);
    }

    /**
     * Load graph into Spark RDDs
     *
     * @param inputRDD                an InputRDD instance
     * @param sparkGraphConfiguration graph configurations
     * @param sparkContext            a JavaSparkContext instance
     * @return vertices in Spark RDD
     */
    public static JavaPairRDD<Object, VertexWritable> loadVertices(
        final InputRDD inputRDD,
        final org.apache.commons.configuration2.Configuration sparkGraphConfiguration,
        JavaSparkContext sparkContext) {
        JavaPairRDD<Object, VertexWritable> loadedGraphRDD = inputRDD.readGraphRDD(sparkGraphConfiguration, sparkContext);
        return loadedGraphRDD;
    }

    /**
     * Create an InputRDD instance based on {@value Constants#GREMLIN_HADOOP_GRAPH_READER} config
     * <p>
     * If {@value Constants#GREMLIN_HADOOP_GRAPH_READER} is of {@link InputRDD} format, instantiate an instance,
     * otherwise, instantiate an {@link InputFormatRDD} instance
     *
     * @param hadoopConfiguration hadoop configurations
     * @return generated InputRDD instance
     */
    public static InputRDD createInputRDD(final Configuration hadoopConfiguration) {
        final InputRDD inputRDD;
        try {
            inputRDD = InputRDD.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, Object.class)) ?
                hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, InputRDD.class, InputRDD.class).newInstance() :
                InputFormatRDD.class.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return inputRDD;
    }
}
