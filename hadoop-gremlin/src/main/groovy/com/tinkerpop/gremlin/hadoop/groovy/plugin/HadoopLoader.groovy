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
package com.tinkerpop.gremlin.hadoop.groovy.plugin

import com.tinkerpop.gremlin.hadoop.structure.hdfs.HDFSTools
import com.tinkerpop.gremlin.hadoop.structure.hdfs.HiddenFileFilter
import com.tinkerpop.gremlin.hadoop.structure.hdfs.TextIterator
import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable
import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator
import com.tinkerpop.gremlin.util.StreamFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.Text

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class HadoopLoader {

    private static final String SPACE = " ";
    private static final String D_SPACE = "(D) ";

    public static void load() {

        FileStatus.metaClass.toString = {
            StringBuilder s = new StringBuilder();
            s.append(((FileStatus) delegate).getPermission()).append(SPACE)
            s.append(((FileStatus) delegate).getOwner()).append(SPACE);
            s.append(((FileStatus) delegate).getGroup()).append(SPACE);
            s.append(((FileStatus) delegate).getLen()).append(SPACE);
            if (((FileStatus) delegate).isDir())
                s.append(D_SPACE);
            s.append(((FileStatus) delegate).getPath().getName());
            return s.toString();
        }

        FileSystem.metaClass.ls = { String path ->
            if (null == path || path.equals("/")) path = ((FileSystem) delegate).getHomeDirectory().toString();
            return ((FileSystem) delegate).globStatus(new Path(path + "/*")).collect {
                it.toString()
            };
        }

        FileSystem.metaClass.mkdir = { String path ->
            ((FileSystem) delegate).mkdirs(new Path(path));
        }

        FileSystem.metaClass.cp = { final String from, final String to ->
            return FileUtil.copy(((FileSystem) delegate), new Path(from), ((FileSystem) delegate), new Path(to), false, new Configuration());
        }

        FileSystem.metaClass.exists = { final String path ->
            return ((FileSystem) delegate).exists(new Path(path));
        }

        FileSystem.metaClass.rm = { final String path ->
            HDFSTools.globDelete((FileSystem) delegate, path, false);
        }

        FileSystem.metaClass.rmr = { final String path ->
            HDFSTools.globDelete((FileSystem) delegate, path, true);
        }

        FileSystem.metaClass.copyToLocal = { final String from, final String to ->
            return ((FileSystem) delegate).copyToLocalFile(new Path(from), new Path(to));
        }

        FileSystem.metaClass.copyFromLocal = { final String from, final String to ->
            return ((FileSystem) delegate).copyFromLocalFile(new Path(from), new Path(to));
        }

        FileSystem.metaClass.mergeToLocal = { final String from, final String to ->
            final FileSystem fs = (FileSystem) delegate;
            final FileSystem local = FileSystem.getLocal(new Configuration());
            final FSDataOutputStream outA = local.create(new Path(to));

            HDFSTools.getAllFilePaths(fs, new Path(from), HiddenFileFilter.instance()).each {
                final FSDataInputStream inA = fs.open(it);
                IOUtils.copyBytes(inA, outA, 8192);
                inA.close();
            }
            outA.close();
        }

        FileSystem.metaClass.head = { final String path, final long totalLines ->
            return ((FileSystem) delegate).head(path, Long.MAX_VALUE, Text.class);
        }

        FileSystem.metaClass.head = { final String path ->
            return ((FileSystem) delegate).head(path, Long.MAX_VALUE, Text.class);
        }

        FileSystem.metaClass.head = {
            final String path, final Class<org.apache.hadoop.io.Writable> writableClass ->
                return ((FileSystem) delegate).head(path, Long.MAX_VALUE, writableClass);
        }

        FileSystem.metaClass.head = {
            final String path, final long totalKeyValues, final Class<org.apache.hadoop.io.Writable> writableClass ->
                // if(writableClass.equals(org.apache.giraph.graph.Vertex.class)) {
                /// return StreamFactory.stream(new GiraphVertexIterator(((FileSystem) delegate).getConf(), new Path(path))).limit(totalKeyValues).iterator();
                // } else
                if (writableClass.equals(ObjectWritable.class)) {
                    return StreamFactory.stream(new ObjectWritableIterator(((FileSystem) delegate).getConf(), new Path(path))).limit(totalKeyValues).iterator();
                } else {
                    return StreamFactory.stream(new TextIterator(((FileSystem) delegate).getConf(), new Path(path))).limit(totalKeyValues).iterator();
                }
        }

        /*FileSystem.metaClass.unzip = { final String from, final String to, final boolean deleteZip ->
            HDFSTools.decompressPath((FileSystem) delegate, from, to, Tokens.BZ2, deleteZip);
        }*/

    }
}
