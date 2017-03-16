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
package org.apache.tinkerpop.gremlin.console.plugin

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteException
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex
import groovy.json.JsonSlurper
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Randall Barnhart (randompi@gmail.com)
 * @deprecated As of release 3.2.4, replaced by {@link org.apache.tinkerpop.gremlin.console.jsr223.GephiRemoteAcceptor}
 */
@Deprecated
class GephiRemoteAcceptor implements RemoteAcceptor {

    private String host = "localhost"
    private int port = 8080
    private String workspace = "workspace1"

    private final Groovysh shell
    private final IO io

    private final Random rand = new Random();
    boolean traversalSubmittedForViz = false
    long vizStepDelay
    private float[] vizStartRGBColor
    private float[] vizDefaultRGBColor
    private char vizColorToFade
    private float vizColorFadeRate
    private float vizStartSize
    private float vizSizeDecrementRate
    private Map vertexAttributes = [:]

    private CloseableHttpClient httpclient = HttpClients.createDefault();

    public GephiRemoteAcceptor(final Groovysh shell, final IO io) {
        this.shell = shell
        this.io = io

        // traversal visualization defaults
        vizStepDelay = 1000;                 // 1 second pause between viz of steps
        vizStartRGBColor = [0.0f, 1.0f, 0.5f]  // light aqua green
        vizDefaultRGBColor = [0.6f, 0.6f, 0.6f]  // light grey
        vizColorToFade = 'g'                 // will fade so blue is strongest
        vizColorFadeRate = 0.7               // the multiplicative rate to fade visited vertices
        vizStartSize = 10
        vizSizeDecrementRate = 0.33f
    }

    @Override
    connect(final List<String> args) throws RemoteException {
        if (args.size() >= 1)
            workspace = args[0]

        if (args.size() >= 2)
            host = args[1]

        if (args.size() >= 3) {
            try {
                port = Integer.parseInt(args[2])
            } catch (Exception ex) {
                throw new RemoteException("Port must be an integer value")
            }
        }

        def vizConfig = " with stepDelay:$vizStepDelay, startRGBColor:$vizStartRGBColor, " +
                "colorToFade:$vizColorToFade, colorFadeRate:$vizColorFadeRate, startSize:$vizStartSize," +
                "sizeDecrementRate:$vizSizeDecrementRate"

        return "Connection to Gephi - http://$host:$port/$workspace" + vizConfig
    }

    @Override
    Object configure(final List<String> args) throws RemoteException {
        if (args.size() < 2 && args[0] != "help")
            throw new RemoteException("Invalid config arguments - check syntax")

        if (args[0] == "help")
            return ":remote config [host <host>|port <port>|workspace <name>|stepDelay <ms>|startRGBColor <0.0,0.0,0.0>|colorToFade [r|g|b]|colorFadeRate <0.0>|sizeDecrementRate <0.0>|startSize <0.0>|visualTraversal <graph>|help]"

        if (args[0] == "host")
            host = args[1]
        else if (args[0] == "port") {
            try {
                port = Integer.parseInt(args[1])
            } catch (Exception ignored) {
                throw new RemoteException("Port must be an integer value")
            }
        } else if (args[0] == "workspace")
            workspace = args[1]
        else if (args[0] == "stepDelay")
            parseVizStepDelay(args[1])
        else if (args[0] == "startRGBColor")
            parseVizStartRGBColor(args[1])
        else if (args[0] == "colorToFade")
            parseVizColorToFade(args[1])
        else if (args[0] == "colorFadeRate")
            parseVizColorFadeRate(args[1])
        else if (args[0] == "sizeDecrementRate")
            parseVizSizeDecrementRate(args[1])
        else if (args[0] == "startSize")
            parseVizStartSize(args[1])
        else if (args[0] == "visualTraversal") {
            def graphVar = shell.interp.context.getVariable(args[1])
            if (!(graphVar instanceof Graph))
                throw new RemoteException("Invalid argument to 'visualTraversal' - first parameter must be a Graph instance")

            def gVar = args.size() == 3 ? args[2] : "vg"
            def theG = GraphTraversalSource.build().with(new GephiTraversalVisualizationStrategy(this)).create(graphVar)
            shell.interp.context.setVariable(gVar, theG)
        } else
            throw new RemoteException("Invalid config arguments - check syntax")

        return "Connection to Gephi - http://$host:$port/$workspace" +
                " with stepDelay:$vizStepDelay, startRGBColor:$vizStartRGBColor, " +
                "colorToFade:$vizColorToFade, colorFadeRate:$vizColorFadeRate, startSize:$vizStartSize," +
                "sizeDecrementRate:$vizSizeDecrementRate"
    }

    @Override
    @CompileStatic
    Object submit(final List<String> args) throws RemoteException {
        final String line = String.join(" ", args)
        if (line.trim() == "clear") {
            clearGraph()
            io.out.println("Gephi workspace cleared")
            return
        }

        // need to clear the vertex attributes
        vertexAttributes.clear()

        // this tells the GraphTraversalVisualizationStrategy that if the line eval's to a traversal it should
        // try to visualize it
        traversalSubmittedForViz = true

        // get the colors/sizes back to basics before trying visualize
        resetColorsSizes()

        final Object o = shell.execute(line)
        if (o instanceof Graph) {
            clearGraph()
            def graph = (Graph) o
            def g = graph.traversal()
            g.V().sideEffect { addVertexToGephi(g, it.get()) }.iterate()
        }

        traversalSubmittedForViz = false
    }

    @Override
    void close() throws IOException {
        httpclient.close()
    }

    /**
     * Visits the last set of vertices traversed and degrades their color and size.
     */
    def updateVisitedVertices(def List except = []) {
        vertexAttributes.keySet().findAll{ vertexId -> !except.contains(vertexId) }.each { String vertexId ->
            def attrs = vertexAttributes[vertexId]
            float currentColor = attrs.color
            currentColor *= vizColorFadeRate

            int currentSize = attrs["size"]
            currentSize = Math.max(vizStartSize, currentSize - (currentSize * vizSizeDecrementRate))

            vertexAttributes.get(vertexId).color = currentColor
            vertexAttributes.get(vertexId).size = currentSize

            changeVertexAttributes(vertexId)
        }
    }

    def changeVertexAttributes(def String vertexId) {
        def props = [:]
        props.put(vizColorToFade.toString(), vertexAttributes[vertexId].color)
        props.put("size", vertexAttributes[vertexId].size)
        updateGephiGraph([cn: [(vertexId): props]])
    }

    /**
     * Visit a vertex traversed and initialize its color and size.
     */
    def visitVertexInGephi(def Vertex v) {
        def props = [:]
        props.put('r', vizStartRGBColor[0])
        props.put('g', vizStartRGBColor[1])
        props.put('b', vizStartRGBColor[2])
        props.put('size', vizStartSize * 2.5)
        props.put('visited', 1)

        updateGephiGraph([cn: [(v.id().toString()): props]])

        vertexAttributes[v.id().toString()] = [color: vizStartRGBColor[fadeColorIndex()], size: vizStartSize * 2.5, touch: 1]
    }

    def fadeColorIndex() {
        if (vizColorToFade == 'r')
            return 0
        else if (vizColorToFade == 'g')
            return 1
        else if (vizColorToFade == 'b')
            return 2
    }

    def addVertexToGephi(def GraphTraversalSource g, def Vertex v, def boolean ignoreEdges = false) {
        // grab the first property value from the strategies of values
        def props = g.V(v).valueMap().next().collectEntries { kv -> [(kv.key): kv.value[0]] }
        props << [label: v.label()]
        props.put('r', vizDefaultRGBColor[0])
        props.put('g', vizDefaultRGBColor[1])
        props.put('b', vizDefaultRGBColor[2])
        props.put('x', rand.nextFloat())
        props.put('y', rand.nextFloat())
        props.put('size', 10)
        props.put('visited', 0)

        // only add if it does not exist in graph already
        if (!getFromGephiGraph([operation: "getNode", id: v.id().toString()]).isPresent())
            updateGephiGraph([an: [(v.id().toString()): props]])

        if (!ignoreEdges) {
            g.V(v).outE().sideEffect {
                addEdgeToGephi(g, it.get())
            }.iterate()
        }
    }

    @CompileStatic
    def addEdgeToGephi(def GraphTraversalSource g, def Edge e) {
        def props = g.E(e).valueMap().next()
        props.put('label', e.label())
        props.put('source', e.outVertex().id().toString())
        props.put('target', e.inVertex().id().toString())
        props.put('directed', true)
        props.put('visited', 0)

        // make sure the in vertex is there but don't add its edges - that will happen later as we are looping
        // all vertices in the graph
        addVertexToGephi(g, e.inVertex(), true)

        // both vertices are definitely there now, so add the edge
        updateGephiGraph([ae: [(e.id().toString()): props]])
    }

    def clearGraph() {
        updateGephiGraph([dn: [filter: "ALL"]])
    }

    def resetColorsSizes() {
        updateGephiGraph([cn: [filter: [nodeAttribute: [attribute: "visited", value: 1]],
                               attributes: [size: 1, r: vizDefaultRGBColor[0], g: vizDefaultRGBColor[1], b: vizDefaultRGBColor[2]]]])
        updateGephiGraph([ce: [filter: [edgeAttribute: [attribute: "visited", value: 1]],
                               attributes: [size: 1, r: vizDefaultRGBColor[0], g: vizDefaultRGBColor[1], b: vizDefaultRGBColor[2]]]])
    }

    def getFromGephiGraph(def Map queryArgs) {
        def requestBuilder = RequestBuilder.get("http://$host:$port/$workspace")
        queryArgs.each { requestBuilder = requestBuilder.addParameter(it.key, it.value) }

        def httpResp = makeRequest(requestBuilder.build())
        def resp = EntityUtils.toString(httpResp.entity)

        // gephi streaming plugin does not set the content type or respect the Accept header - treat as text
        if (resp.isEmpty())
            return Optional.empty()
        else
            return Optional.of(new JsonSlurper().parseText(resp))
    }

    def updateGephiGraph(def Map postBody) {
        def requestBuilder = RequestBuilder.post("http://$host:$port/$workspace")
                                           .addParameter("format", "JSON")
                                           .addParameter("operation", "updateGraph")
                                           .setEntity(new StringEntity(JsonOutput.toJson(postBody)))
        EntityUtils.consume(makeRequest(requestBuilder.build()).entity)
    }

    private CloseableHttpResponse makeRequest(HttpUriRequest request) {
        def httpResp = httpclient.execute(request)
        if (httpResp.getStatusLine().getStatusCode() == 200) {
            return httpResp
        } else {
            def resp = EntityUtils.toString(httpResp.entity)
            throw new RuntimeException("Unsuccessful request to Gephi - [${httpResp.getStatusLine().getStatusCode()}] ${httpResp.getStatusLine().getReasonPhrase()} - $resp")
        }
    }

    @Override
    public String toString() {
        return "Gephi - [$workspace]"
    }

    private void parseVizStepDelay(String arg) {
        try {
            vizStepDelay = Long.parseLong(arg)
        } catch (Exception ignored) {
            throw new RemoteException("The stepDelay must be a long value")
        }
    }

    private void parseVizStartRGBColor(String arg) {
        try {
            vizStartRGBColor = arg[1..-2].tokenize(',')*.toFloat()
            assert (vizStartRGBColor.length == 3)
        } catch (Exception ignored) {
            throw new RemoteException("The vizStartRGBColor must be an array of 3 float values, e.g. [0.0,1.0,0.5]")
        }
    }

    private void parseVizColorToFade(String arg) {
        try {
            vizColorToFade = arg.charAt(0).toLowerCase();
            assert (vizColorToFade == 'r' || vizColorToFade == 'g' || vizColorToFade == 'b')
        } catch (Exception ignored) {
            throw new RemoteException("The vizColorToFade must be one character value among: r, g, b, R, G, B")
        }
    }

    private void parseVizColorFadeRate(String arg) {
        try {
            vizColorFadeRate = Float.parseFloat(arg)
        } catch (Exception ignored) {
            throw new RemoteException("The colorFadeRate must be a float value")
        }
    }

    private void parseVizSizeDecrementRate(String arg) {
        try {
            vizSizeDecrementRate = Float.parseFloat(arg)
        } catch (Exception ignored) {
            throw new RemoteException("The sizeDecrementRate must be a float value")
        }
    }

    private void parseVizStartSize(String arg) {
        try {
            vizStartSize = Float.parseFloat(arg)
        } catch (Exception ignored) {
            throw new RemoteException("The startSize must be a float value")
        }
    }
}
