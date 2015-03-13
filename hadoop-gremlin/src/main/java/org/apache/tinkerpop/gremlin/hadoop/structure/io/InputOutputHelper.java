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

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptOutputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class InputOutputHelper {

    private InputOutputHelper() {

    }

    public static Class<? extends InputFormat> getInputFormat(final Class<? extends OutputFormat> outputFormat) {
        if (outputFormat.equals(GryoOutputFormat.class))
            return GryoInputFormat.class;
        else if (outputFormat.equals(GraphSONOutputFormat.class))
            return GraphSONInputFormat.class;
        else if (outputFormat.equals(ScriptOutputFormat.class))
            return ScriptInputFormat.class;
        else
            throw new IllegalArgumentException("The provided output format does not have a known input format: " + outputFormat.getCanonicalName());
    }

    public static Class<? extends OutputFormat> getOutputFormat(final Class<? extends InputFormat> inputFormat) {
        if (inputFormat.equals(GryoInputFormat.class))
            return GryoOutputFormat.class;
        else if (inputFormat.equals(GraphSONInputFormat.class))
            return GraphSONOutputFormat.class;
        else if (inputFormat.equals(ScriptInputFormat.class))
            return ScriptOutputFormat.class;
        else
            throw new IllegalArgumentException("The provided input format does not have a known output format: " + inputFormat.getCanonicalName());
    }
}
