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

package org.apache.tinkerpop.gremlin.structure.io;

import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Storage {

    public List<String> ls();

    public List<String> ls(final String location);

    public boolean mkdir(final String location);

    public boolean cp(final String fromLocation, final String toLocation);

    public boolean exists(final String location);

    public boolean rm(final String location);

    public boolean rmr(final String location);

    public <V> Iterator<V> head(final String location, final int totalLines, final Class<V> objectClass);

    public default Iterator<Object> head(final String location) {
        return this.head(location, Object.class);
    }

    public default Iterator<Object> head(final String location, final int totalLines) {
        return this.head(location, totalLines, Object.class);
    }

    public default <V> Iterator<V> head(final String location, final Class<V> objectClass) {
        return this.head(location, Integer.MAX_VALUE, objectClass);
    }

  /*

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

     */
}
