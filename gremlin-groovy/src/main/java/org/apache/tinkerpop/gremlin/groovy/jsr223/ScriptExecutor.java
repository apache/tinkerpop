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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import javax.script.Bindings;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;

/**
 * Executes a Gremlin script from the command line. Takes a path to the Gremlin script file as the first argument.
 * Remaining arguments are treated as parameters to the script, where they are batched up into an array and passed
 * in as a single parameter to the script named "args".
 *
 * @author Pavel A. Yaskevich
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.1, replaced by direct execution via gremlin.sh
 */
@Deprecated
public class ScriptExecutor {
    public static void main(final String[] arguments) throws IOException {
        if (arguments.length == 0) {
            System.out.println("Usage: <path_to_gremlin_script> <argument1> <argument2> ...");
        } else {
            evaluate(new FileReader(arguments[0]), Arrays.asList(arguments).subList(1, arguments.length));
        }
    }

    protected static void evaluate(final Reader reader, final List<String> arguments) {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("args", arguments.toArray());

        try {
            engine.eval(reader, bindings);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
