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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * An interface that defines a {@link Step} as one that handles IO based operations for a {@link Graph}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ReadWriting extends Configuring {

    /**
     * Determines the mode of the the IO operation as being for reading or writing (or by default "unset")
     */
    public enum Mode {
        UNSET,
        READING,
        WRITING
    }

    /**
     * Get the file location to write to.
     */
    public String getFile();

    public void setMode(final Mode mode);

    public Mode getMode();
}
