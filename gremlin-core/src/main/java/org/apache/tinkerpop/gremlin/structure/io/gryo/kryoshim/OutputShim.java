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
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim;

/**
 * A minimal {@link org.apache.tinkerpop.shaded.kryo.io.Output}-like abstraction.
 * See that class for method documentation.
 */
public interface OutputShim {

    public void writeByte(final byte b);

    public void writeBytes(final byte[] array, final int offset, final int count);

    public void writeString(final String s);

    public void writeLong(final long l);

    public void writeInt(final int i);

    public void writeDouble(final double d);

    public void writeShort(final int s);

    public void flush();
}
