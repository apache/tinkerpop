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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.util.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Rule implements Writable, Serializable {

    public enum Operation {
        OR {
            public Boolean compute(final Object first, final Object second) {
                if (null == first)
                    return (Boolean) second;
                else if (null == second)
                    return (Boolean) first;
                else
                    return (Boolean) first || (Boolean) second;
            }
        }, AND {
            public Boolean compute(final Object first, final Object second) {
                if (null == first)
                    return (Boolean) second;
                else if (null == second)
                    return (Boolean) first;
                else
                    return (Boolean) first && (Boolean) second;
            }
        }, INCR {
            public Long compute(final Object first, final Object second) {
                if (null == first)
                    return (Long) second;
                else if (null == second)
                    return (Long) first;
                else
                    return (Long) first + (Long) second;

            }
        }, SET {
            public Object compute(final Object first, final Object second) {
                return null == second ? first : second;
            }
        }, NO_OP {
            public Object compute(final Object first, final Object second) {
                return null == first ? second : first;
            }
        };

        public abstract Object compute(final Object first, final Object second);
    }

    private Operation operation;
    private Object object;

    public Rule(final Operation operation, final Object object) {
        this.operation = operation;
        this.object = object;
    }

    public Operation getOperation() {
        return this.operation;
    }

    public <R> R getObject() {
        return (R) this.object;
    }

    public String toString() {
        return "rule[" + this.operation + ":" + this.object + "]";
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, this.operation.ordinal());
        final byte[] objectBytes = Serializer.serializeObject(this.object);
        WritableUtils.writeVInt(output, objectBytes.length);
        output.write(objectBytes);
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        this.operation = Operation.values()[WritableUtils.readVInt(input)];
        final int objectLength = WritableUtils.readVInt(input);
        final byte[] objectBytes = new byte[objectLength];
        for (int i = 0; i < objectLength; i++) {
            objectBytes[i] = input.readByte();
        }
        try {
            this.object = Serializer.deserializeObject(objectBytes);
        } catch (final ClassNotFoundException e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
