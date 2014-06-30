package com.tinkerpop.gremlin.console.plugin

import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.Vertex
import groovy.json.JsonSlurper
import groovyx.net.http.HTTPBuilder
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO

import static groovyx.net.http.ContentType.JSON

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GephiRemoteAcceptor implements RemoteAcceptor {

    private String host = "localhost"
    private int port = 8080
    private String workspace = "workspace0"

    private final Groovysh shell
    private final IO io

    public GephiRemoteAcceptor(final Groovysh shell, final IO io) {
        this.shell = shell
        this.io = io
    }

    @Override
    Object connect(final List<String> args) {
        if (args.size() >= 1)
            workspace = args[0]

        if (args.size() >= 2)
            host = args[1]

        if (args.size() >= 3) {
            try {
                port = Integer.parseInt(args[2])
            } catch (Exception ex) {
                return "port must be an integer value"
            }
        }

        return "connection to Gephi - http://$host:$port/$workspace"
    }

    @Override
    Object configure(final List<String> args) {
        if (args.size() != 2)
            return "expects [host <hostname>|port <port number>|workspace <name>]"

        if (args[0] == "host")
            host = args[1]
        else if (args[0] == "port") {
            try {
                port = Integer.parseInt(args[1])
            } catch (Exception ex) {
                return "port must be an integer value"
            }
        } else if (args[0] == "workspace")
            workspace = args[1]
        else
            return "expects [host <hostname>|port <port number>|workspace <name>]"

        return "connection to Gephi - http://$host:$port/$workspace"
    }

    @Override
    Object submit(final List<String> args) {
        final String line = String.join(" ", args)
        final Object o = shell.execute(line)
        if (o instanceof Graph) {
            clearGraph()
            def g = (Graph) o
            g.V().sideEffect { addVertexToGephi(it.get()) }.iterate()
        }
    }

    @Override
    void close() throws IOException {

    }

    def addVertexToGephi(def Vertex v, def boolean ignoreEdges = false) {
        def props = v.values()
        props.put('label', v.label())

        // only add if it does not exist in graph already
        if (!getFromGephiGraph([operation: "getNode", id: v.id().toString()]).isPresent())
            updateGephiGraph([an: [(v.id().toString()): props]])

        if (!ignoreEdges) {
            v.outE().sideEffect {
                addEdgeToGephi(it.get())
            }.iterate()
        }
    }

    def addEdgeToGephi(def Edge e) {
        def props = e.values()
        props.put('label', e.label())
        props.put('source', e.outV().id().next().toString())
        props.put('target', e.inV().id().next().toString())
        props.put('directed', true)

        // make sure the in vertex is there but don't add its edges - that will happen later as we are looping
        // all vertices in the graph
        addVertexToGephi(e.inV().next(), true)

        // both vertices are definitely there now, so add the edge
        updateGephiGraph([ae: [(e.id().toString()): props]])
    }

    def clearGraph() {
        updateGephiGraph([dn: [filter: "ALL"]])
    }

    def getFromGephiGraph(def Map queryArgs) {
        def http = new HTTPBuilder("http://$host:$port/")
        def resp = http.get(path: "/$workspace", query: queryArgs).getText()

        // gephi streaming plugin does not set the content type or respect the Accept header - treat as text
        if (resp.isEmpty())
            return Optional.empty()
        else
            return Optional.of(new JsonSlurper().parseText(resp))
    }

    def updateGephiGraph(def Map postBody) {
        def http = new HTTPBuilder("http://$host:$port/")
        http.post(path: "/$workspace", requestContentType: JSON, body: postBody, query: [format: "JSON", operation: "updateGraph"])
    }
}
