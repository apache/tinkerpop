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

package org.apache.tinkerpop.gremlin.hadoop.process.computer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.junit.Test;

import java.io.File;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AbstractHadoopGraphComputerTest {

    @Test
    public void shouldCopyDirectoriesCorrectly() throws Exception {
        final String hdfsName = this.getClass().getSimpleName() + "-hdfs";
        final String localName = this.getClass().getSimpleName() + "-local";
        final FileSystem fs = FileSystem.get(new Configuration());
        File path;

        path = new File(System.getProperty("java.io.tmpdir") + "/" + localName);
        if (!path.exists()) {
            assertTrue(path.mkdir());
        }

        File tempFile1 = new File(path, "test1.txt");
        tempFile1.delete();
        assertTrue("Expected "+tempFile1+" file was created successfully", tempFile1.createNewFile());
        assertTrue("Expected "+tempFile1+" existed", tempFile1.exists());

        File tempFile2 = new File(path, "test2.txt");
        tempFile2.delete();
        assertTrue("Expected "+tempFile2+" file was created successfully", tempFile2.createNewFile());
        assertTrue("Expected "+tempFile2+" existed", tempFile2.exists());

        if (fs.exists(new Path("target/" + hdfsName)))
            assertTrue(fs.delete(new Path("target/" + hdfsName), true));
        fs.copyFromLocalFile(true, new Path(tempFile1.getAbsolutePath()), new Path("target/" + hdfsName + "/test1.dat"));
        fs.copyFromLocalFile(true, new Path(tempFile2.getAbsolutePath()), new Path("target/" + hdfsName + "/test2.dat"));
        assertTrue(fs.exists(new Path("target/" + hdfsName + "/test1.dat")));
        assertTrue(fs.exists(new Path("target/" + hdfsName + "/test2.dat")));
        assertTrue(fs.exists(new Path("target/" + hdfsName)));
        assertTrue(fs.isDirectory(new Path("target/" + hdfsName)));
        assertFalse(tempFile1.exists());
        assertFalse(tempFile2.exists());

        assertTrue("Expected "+path+" directory existed", path.exists());
        assertTrue("Expected "+path+" directory was deleted", path.delete());

        assertTrue(fs.exists(new Path("target/" + hdfsName + "/test1.dat")));
        assertTrue(fs.exists(new Path("target/" + hdfsName + "/test2.dat")));
        assertTrue(fs.exists(new Path("target/" + hdfsName)));
        assertTrue(fs.isDirectory(new Path("target/" + hdfsName)));
        /////
        final String hadoopGremlinLibsRemote = "hadoop-gremlin-" + Gremlin.version() + "-libs";
        final File localDirectory = new File(System.getProperty("java.io.tmpdir") + "/" + hadoopGremlinLibsRemote);
        final File localLibDirectory = new File(localDirectory.getAbsolutePath() + "/" + hdfsName);
        if (localLibDirectory.exists()) {
            Stream.of(localLibDirectory.listFiles()).forEach(File::delete);
            assertTrue(localLibDirectory.delete());
        }
        assertFalse(localLibDirectory.exists());
        assertEquals(localLibDirectory, AbstractHadoopGraphComputer.copyDirectoryIfNonExistent(fs, "target/" + hdfsName));
        assertTrue(localLibDirectory.exists());
        assertTrue(localLibDirectory.isDirectory());
        assertEquals(2, Stream.of(localLibDirectory.listFiles()).filter(file -> file.getName().endsWith(".dat")).count());
    }
}
