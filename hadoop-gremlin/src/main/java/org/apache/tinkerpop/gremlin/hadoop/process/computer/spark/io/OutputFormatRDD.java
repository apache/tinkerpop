/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.io;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import scala.Tuple2;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OutputFormatRDD implements OutputRDD {

    @Override
    public void writeGraphRDD(final Configuration configuration, final JavaPairRDD<Object, VertexWritable> graphRDD) {
        final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(configuration);
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        if (null != outputLocation) {
            // map back to a <nullwritable,vertexwritable> stream for output
            graphRDD.mapToPair(tuple -> new Tuple2<>(NullWritable.get(), tuple._2()))
                    .saveAsNewAPIHadoopFile(outputLocation + "/" + Constants.HIDDEN_G,
                            NullWritable.class,
                            VertexWritable.class,
                            (Class<OutputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class), hadoopConfiguration);
        }
    }
}
