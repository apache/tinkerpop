package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.exception.ConnectionException;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.ser.SerTokens;
import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverGremlinPlugin extends AbstractGremlinPlugin {

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + Cluster.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + ConnectionException.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + RequestMessage.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + SerTokens.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "tinkerpop.server";
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return null == this.shell ? Optional.empty() : Optional.of(new DriverRemoteAcceptor(this.shell));
    }
}
