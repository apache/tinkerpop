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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.List;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PathSerializer extends SimpleTypeSerializer<Path> {

    public PathSerializer() {
        super(DataType.PATH);
    }

    @Override
    protected Path readValue(final Buffer buffer, final GraphBinaryReader context) throws SerializationException {
        final MutablePath path = (MutablePath) MutablePath.make();
        final List<Set<String>> labels = context.read(buffer);
        final List<Object> objects = context.read(buffer);

        if (labels.size() != objects.size())
            throw new IllegalStateException("Format for Path object requires that the labels and objects fields be of the same length");

        for (int ix = 0; ix < labels.size(); ix++) {
            path.extend(objects.get(ix), labels.get(ix));
        }

        return ReferenceFactory.detach(path);
    }

    @Override
    protected void writeValue(final Path value, final Buffer buffer, final GraphBinaryWriter context) throws SerializationException {
        context.write(value.labels(), buffer);
        context.write(value.objects(), buffer);
    }
}
