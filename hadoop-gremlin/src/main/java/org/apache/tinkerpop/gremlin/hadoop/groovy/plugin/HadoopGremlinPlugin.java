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
package com.tinkerpop.gremlin.hadoop.groovy.plugin;

import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import com.tinkerpop.gremlin.hadoop.Constants;
import com.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphGraphComputer;
import com.tinkerpop.gremlin.hadoop.process.computer.mapreduce.MapReduceGraphComputer;
import com.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.hadoop.structure.hdfs.HDFSTools;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import com.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import com.tinkerpop.gremlin.hadoop.structure.io.kryo.KryoInputFormat;
import com.tinkerpop.gremlin.hadoop.structure.io.script.ScriptInputFormat;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.GroupCountMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopGremlinPlugin extends AbstractGremlinPlugin {

    private static final Set<String> IMPORTS = new HashSet<String>() {{
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
        add(IMPORT_SPACE + KryoInputFormat.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + GraphSONInputFormat.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + ScriptInputFormat.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + HDFSTools.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + GroupCountMapReduce.class.getPackage().getName() + DOT_STAR);
        ////
        add(IMPORT_SPACE + GiraphGraphComputer.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + MapReduceGraphComputer.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "tinkerpop.hadoop";
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
        try {
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", JobClient.class.getName()));
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", Job.class.getName()));
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", GiraphGraphComputer.class.getName()));
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", MapReduceGraphComputer.class.getName()));
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", HadoopGraph.class.getName()));
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