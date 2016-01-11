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
package org.apache.tinkerpop.gremlin.groovy.jsr223.customizer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.TypeDescription
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/**
 * A sandbox implementation that uses an external file to control the settings of the sandbox. The file is yaml based
 * and looks like:
 *
 * <pre>
 * {@code
 * autoTypeUnknown: true
 * methodWhiteList:
 *   - java\.util\..*
 *   - org\.codehaus\.groovy\.runtime\.DefaultGroovyMethods
 * staticVariableTypes:
 *   graph: org.apache.tinkerpop.structure.Graph
 * }
 * </pre>
 *
 * The file is assumed to be at the root of execution and should be called {@code sandbox.yaml}.  Its location can be
 * overriden by a system property called {@code gremlinServerSandbox}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class FileSandboxExtension extends AbstractSandboxExtension {
    public static final String GREMLIN_SERVER_SANDBOX = "gremlinServerSandbox"

    private static final Logger logger = LoggerFactory.getLogger(FileSandboxExtension.class)

    private boolean autoTypeUnknown
    private List<String> methodWhiteList
    private Map<String,Class<?>> staticVariableTypes

    public FileSandboxExtension() {
        def file = new File(System.getProperty(GREMLIN_SERVER_SANDBOX, "sandbox.yaml"))

        if (!file.exists())
            throw new RuntimeException("FileSandboxExtension could not find its configuration file at $file")

        final Settings settings = Settings.read(file)

        autoTypeUnknown = settings.autoTypeUnknown
        methodWhiteList = settings.methodWhiteList
        staticVariableTypes = settings.staticVariableTypes.collectEntries { kv ->
            try {
                return [(kv.key): Class.forName(kv.value)]
            } catch (Exception ex) {
                logger.error("Could not convert ${kv.value} to a Class for variable ${kv.key}", ex)
                throw ex
            }
        }
    }

    @Override
    List<String> getMethodWhiteList() {
        return methodWhiteList
    }

    @Override
    Map<String, Class<?>> getStaticVariableTypes() {
        return staticVariableTypes
    }

    @Override
    boolean allowAutoTypeOfUnknown() {
        return autoTypeUnknown
    }

    static class Settings {
        public boolean autoTypeUnknown
        public List<String> methodWhiteList
        public Map<String,String> staticVariableTypes

        public static Settings read(final File file) throws Exception {
            final Constructor constructor = new Constructor(Settings.class)
            final TypeDescription settingsDescription = new TypeDescription(Settings.class)
            settingsDescription.putListPropertyType("methodWhiteList", String.class)
            settingsDescription.putMapPropertyType("staticVariableTypes", String.class, String.class)
            constructor.addTypeDescription(settingsDescription)

            final Yaml yaml = new Yaml(constructor)
            return yaml.loadAs(new FileInputStream(file), Settings.class)
        }
    }
}
