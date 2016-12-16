/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.hadoop.structure.io.partitioner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.tinkerpop.gremlin.structure.Partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PartitionerInputSplit extends InputSplit implements Writable {

    private String location;
    private String id;

    public PartitionerInputSplit() {
        // necessary for serialization/writable
    }

    public PartitionerInputSplit(final Partition partition) {
        this.location = partition.location().getHostAddress();
        this.id = partition.id();
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{this.location};
    }

    public String getPartitionId() {
        return this.id;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, this.location);
        WritableUtils.writeString(dataOutput, this.id);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.location = WritableUtils.readString(dataInput);
        this.id = WritableUtils.readString(dataInput);
    }


    @Override
    public String toString() {
        return this.location + "::" + this.id;
    }
}
