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

package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FileSystemStorageCheck extends AbstractStorageCheck {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportHeadMethods() throws Exception {
        // Make sure Spark is shut down before deleting its files and directories,
        // which are locked under Windows and fail the tests. See FileSystemStorageCheck
        graph.configuration().setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);

        final Storage storage = FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(graph.configuration()));
        final String inputLocation = Constants.getSearchGraphLocation(graph.configuration().getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION), storage).get();
        final String outputLocation = graph.configuration().getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        // TestHelper creates the directory and we need it not to exist
        deleteDirectory(outputLocation);
        super.checkHeadMethods(storage, inputLocation, outputLocation, InputOutputHelper.getInputFormat((Class) Class.forName(graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER))), SequenceFileInputFormat.class);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportRemoveAndListMethods() throws Exception {
        // Make sure Spark is shut down before deleting its files and directories,
        // which are locked under Windows and fail the tests. See FileSystemStorageCheck
        graph.configuration().setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);

        final Storage storage = FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(graph.configuration()));
        final String outputLocation = graph.configuration().getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        super.checkRemoveAndListMethods(storage, outputLocation);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportCopyMethods() throws Exception {
        // Make sure Spark is shut down before deleting its files and directories,
        // which are locked under Windows and fail the tests. See FileSystemStorageCheck
        graph.configuration().setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);

        final Storage storage = FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(graph.configuration()));
        final String outputLocation = graph.configuration().getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        final String newOutputLocation = TestHelper.makeTestDataDirectory(FileSystemStorageCheck.class, "new-location-for-copy");
        // TestHelper creates the directory and we need it not to exist
        deleteDirectory(newOutputLocation);
        super.checkCopyMethods(storage, outputLocation, newOutputLocation, InputOutputHelper.getInputFormat((Class) Class.forName(graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER))), SequenceFileInputFormat.class);

    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotHaveResidualDataInStorage() throws Exception {
        // Make sure Spark is shut down before deleting its files and directories,
        // which are locked under Windows and fail the tests. See FileSystemStorageCheck
        graph.configuration().setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);

        final Storage storage = FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(graph.configuration()));
        final String outputLocation = graph.configuration().getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        super.checkResidualDataInStorage(storage, outputLocation);
    }

    @Test
    public void shouldSupportDirectoryFileDistinction() throws Exception {
        // Make sure Spark is shut down before deleting its files and directories,
        // which are locked under Windows and fail the tests. See FileSystemStorageCheck
        graph.configuration().setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);

        final Storage storage = FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(graph.configuration()));
        final String directory1 = TestHelper.makeTestDataDirectory(FileSystemStorageCheck.class, "directory1");
        final String directory2 = TestHelper.makeTestDataDirectory(FileSystemStorageCheck.class, "directory2");
        for (int i = 0; i < 10; i++) {
            new File(directory1, "file1-" + i + ".txt.bz").createNewFile();
        }
        for (int i = 0; i < 5; i++) {
            new File(directory2, "file2-" + i + ".txt.bz").createNewFile();
        }
        super.checkFileDirectoryDistinction(storage, directory1, directory2);
        deleteDirectory(directory1);
        deleteDirectory(directory2);
    }

    private static void deleteDirectory(final String location) throws IOException {
        // TestHelper creates the directory and we need it not to exist
        assertTrue(new File(location).isDirectory());
        assertTrue(new File(location).exists());
        FileUtils.deleteDirectory(new File(location));
        assertFalse(new File(location).exists());
    }
}