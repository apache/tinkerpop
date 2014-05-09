package com.tinkerpop.gremlin.groovy.console.commands

import com.tinkerpop.gremlin.driver.Cluster
import com.tinkerpop.gremlin.groovy.console.Mediator
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Configure a remote connection to a Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class RemoteCommand extends ComplexCommandSupport {
    private final Mediator mediator
    private Cluster currentCluster

    public RemoteCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":remote", ":rem", ["current", "connect"], "current")
        this.mediator = mediator

        // initialize with a localhost connection
        currentCluster = Cluster.open()
        currentCluster.init()
        this.mediator.clusterSelected(currentCluster)
    }

    def Object do_connect = { List<String> arguments ->
        Cluster.Builder builder
        final String line = String.join(" ", arguments)

        try {
            final InetAddress addy = InetAddress.getByName(line)
            builder = Cluster.create(addy.getHostAddress());
        } catch (UnknownHostException e) {
            // not a hostname - try to treat it as a property file
            try {
                builder = Cluster.create(new File(line))
            } catch (FileNotFoundException fnfe) {
                return "could not configure remote - arguments must be a resolvable host or a configuration file";
            }
        }

        currentCluster = builder.build();
        this.mediator.clusterSelected(currentCluster);

        return String.format("connected - " + currentCluster)
    }

    def Object do_current = {
        return String.format("remote - " + currentCluster)
    }
}
