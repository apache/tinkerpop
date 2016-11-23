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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultScriptCustomizerTest {

    @Test
    public void shouldOpenViaPropertiesFileConfig() throws IOException {
        final File scriptFile1 = TestHelper.generateTempFileFromResource(DefaultScriptCustomizerTest.class, "script-customizer-1.groovy", ".groovy");
        final File scriptFile2 = TestHelper.generateTempFileFromResource(DefaultScriptCustomizerTest.class, "script-customizer-2.groovy", ".groovy");
        final Set<File> files = new HashSet<>();
        files.add(scriptFile1);
        files.add(scriptFile2);
        final ScriptCustomizer scripts = new DefaultScriptCustomizer(files);

        final Collection<List<String>> linesInFiles = scripts.getScripts();
        final String scriptCombined = linesInFiles.stream().flatMap(Collection::stream).map(s -> s + System.lineSeparator()).reduce("", String::concat);
        assertEquals("x = 1 + 1" +  System.lineSeparator() +
                     "y = 10 * x" +   System.lineSeparator() +
                     "z = 1 + x + y" +  System.lineSeparator() +
                     "l = g.V(z).out()" +  System.lineSeparator() +
                     "        .group().by('name')" + System.lineSeparator(), scriptCombined);

    }
}
