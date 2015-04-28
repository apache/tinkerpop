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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.TestFileReaderWriterHelper;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoRecordReaderWriterTest {
    @Test
    public void shouldSplitFileAndWriteProperSplits() throws Exception {
        for (int numberOfSplits = 1; numberOfSplits < 10; numberOfSplits++) {
            final File testFile = new File(HadoopGraphProvider.PATHS.get("grateful-dead.kryo"));
            System.out.println("Testing: " + testFile + " (splits " + numberOfSplits + ")");
            final List<FileSplit> splits = TestFileReaderWriterHelper.generateFileSplits(testFile, numberOfSplits);
            TestFileReaderWriterHelper.validateFileSplits(splits, GryoInputFormat.class, Optional.of(GryoOutputFormat.class));
        }
    }
}
