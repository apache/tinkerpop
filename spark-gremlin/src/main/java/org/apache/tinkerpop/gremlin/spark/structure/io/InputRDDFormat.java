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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InputRDDFormat extends InputFormat<NullWritable, VertexWritable> {

    public InputRDDFormat() {

    }

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return Collections.singletonList(new InputSplit() {
            @Override
            public long getLength() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public String[] getLocations() throws IOException, InterruptedException {
                return new String[0];
            }
        });
    }

    @Override
    public RecordReader<NullWritable, VertexWritable> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            final org.apache.hadoop.conf.Configuration hadoopConfiguration = taskAttemptContext.getConfiguration();
            final SparkConf sparkConfiguration = new SparkConf();
            sparkConfiguration.setAppName(UUID.randomUUID().toString());
            hadoopConfiguration.forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
            final InputRDD inputRDD = (InputRDD) Class.forName(sparkConfiguration.get(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD)).newInstance();
            final JavaSparkContext javaSparkContext = new JavaSparkContext(SparkContext.getOrCreate(sparkConfiguration));
            Spark.create(javaSparkContext.sc());
            final Iterator<Tuple2<Object, VertexWritable>> iterator = inputRDD.readGraphRDD(ConfUtil.makeApacheConfiguration(taskAttemptContext.getConfiguration()), javaSparkContext).toLocalIterator();
            return new RecordReader<NullWritable, VertexWritable>() {
                @Override
                public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

                }

                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException {
                    return iterator.hasNext();
                }

                @Override
                public NullWritable getCurrentKey() throws IOException, InterruptedException {
                    return NullWritable.get();
                }

                @Override
                public VertexWritable getCurrentValue() throws IOException, InterruptedException {
                    return iterator.next()._2();
                }

                @Override
                public float getProgress() throws IOException, InterruptedException {
                    return 1.0f; // TODO: make this dynamic (how? its an iterator.)
                }

                @Override
                public void close() throws IOException {

                }
            };
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IOException(e.getMessage(), e);
        }

    }

    /*private static class PartitionInputSplit extends InputSplit {

        private final Partition partition;

        public PartitionInputSplit(final Partition partition) {
            this.partition = partition;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        public Partition getPartition() {
            return this.partition;
        }
    }*/
}
