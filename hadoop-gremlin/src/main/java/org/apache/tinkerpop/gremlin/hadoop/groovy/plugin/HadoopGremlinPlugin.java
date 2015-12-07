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
package org.apache.tinkerpop.gremlin.hadoop.groovy.plugin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.mapreduce.MapReduceGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.hdfs.HDFSTools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopGremlinPlugin extends AbstractGremlinPlugin {

    protected static String NAME = "tinkerpop.hadoop";

    protected static final Set<String> IMPORTS = new HashSet<String>() {{
        add("import org.apache.hadoop.hdfs.*");
        add("import org.apache.hadoop.conf.*");
        add("import org.apache.hadoop.fs.*");
        add("import org.apache.hadoop.util.*");
        add("import org.apache.hadoop.io.*");
        add("import org.apache.hadoop.io.compress.*");
        add("import org.apache.hadoop.mapreduce.lib.input.*");
        add("import org.apache.hadoop.mapreduce.lib.output.*");
        add("import org.apache.log4j.*");
        add(IMPORT_SPACE + Constants.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + HadoopConfiguration.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + ConfUtil.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + VertexWritable.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + GryoInputFormat.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + GraphSONInputFormat.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + ScriptInputFormat.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + HDFSTools.class.getPackage().getName() + DOT_STAR);
        ////
        add(IMPORT_SPACE + MapReduceGraphComputer.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
        try {
            pluginAcceptor.eval(HadoopLoader.class.getCanonicalName() + ".load()");

            pluginAcceptor.addBinding("hdfs", FileSystem.get(new Configuration()));
            pluginAcceptor.addBinding("local", FileSystem.getLocal(new Configuration()));
            if (null == System.getenv(Constants.HADOOP_GREMLIN_LIBS))
                HadoopGraph.LOGGER.warn("Be sure to set the environmental variable: " + Constants.HADOOP_GREMLIN_LIBS);
            else
                HadoopGraph.LOGGER.info(Constants.HADOOP_GREMLIN_LIBS + " is set to: " + System.getenv(Constants.HADOOP_GREMLIN_LIBS));
        } catch (final Exception e) {
            throw new PluginInitializationException(e.getMessage(), e);
        }
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return Optional.of(new HadoopRemoteAcceptor(this.shell));
    }
}