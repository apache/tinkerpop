package com.tinkerpop.gremlin.giraph.groovy.plugin;


import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphRunner;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.io.graphson.GraphSONVertexInputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoVertexInputFormat;
import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.Artifact;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupCountMapReduce;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGremlinPlugin extends AbstractGremlinPlugin {

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + GiraphGraph.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + GiraphGraphComputer.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + KryoVertexInputFormat.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + GraphSONVertexInputFormat.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + Constants.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + HDFSTools.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + GroupCountMapReduce.class.getPackage().getName() + DOT_STAR);
        add("import org.apache.hadoop.hdfs.*");
        add("import org.apache.hadoop.conf.*");
        add("import org.apache.hadoop.fs.*");
        add("import org.apache.hadoop.util.*");
        add("import org.apache.hadoop.io.*");
        add("import org.apache.hadoop.io.compress.*");
        add("import org.apache.hadoop.mapreduce.lib.input.*");
        add("import org.apache.hadoop.mapreduce.lib.output.*");
        add("import org.apache.log4j.*");
    }};

    @Override
    public String getName() {
        return "giraph";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        super.pluginTo(pluginAcceptor);
        pluginAcceptor.addImports(IMPORTS);
        try {
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", JobClient.class.getName()));
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", GiraphGraphRunner.class.getName()));
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", GiraphJob.class.getName()));
            pluginAcceptor.eval(HadoopLoader.class.getCanonicalName() + ".load()");

            pluginAcceptor.addBinding("hdfs", FileSystem.get(new Configuration()));
            pluginAcceptor.addBinding("local", FileSystem.getLocal(new Configuration()));
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    @Override
    public Optional<Set<Artifact>> additionalDependencies() {
        return Optional.of(new HashSet<>(Arrays.asList(new Artifact("org.apache.hadoop", "hadoop-core", "1.2.1"))));
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return Optional.of(new GiraphRemoteAcceptor(this.shell));
    }
}