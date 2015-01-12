package com.tinkerpop.gremlin.console.plugin

import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor
import com.tinkerpop.gremlin.groovy.plugin.RemoteException
import com.tinkerpop.gremlin.process.Traversal
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
 * @author Randall Barnhart (randompi@gmail.com)
 */
class GephiRemoteAcceptor implements RemoteAcceptor {

    private String host = "localhost"
    private int port = 8080
    private String workspace = "workspace0"

    private final Groovysh shell
    private final IO io

    private long vizStepDelay
    private float[] vizStartRGBColor
    private char vizColorToFade
    private float vizColorFadeRate
    private Map<String, Float> fadingVertexColors;

    public GephiRemoteAcceptor(final Groovysh shell, final IO io) {
        this.shell = shell
        this.io = io

        // traversal visualization defaults
        vizStepDelay = 1000;                 // 1 second pause between viz of steps
        vizStartRGBColor = [0.0f, 1.0f, 0.5f]  // light aqua green
        vizColorToFade = 'g'                 // will fade so blue is strongest
        vizColorFadeRate = 0.7               // the multiplicative rate to fade visited vertices
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

        String vizConfig = " with stepDelay:$vizStepDelay, startRGBColor:$vizStartRGBColor, " +
                "colorToFade:$vizColorToFade, colorFadeRate:$vizColorFadeRate"
        if (args.size() >= 4) {
            if (args.size() > 7) {
                vizConfig = configVizOptions(args.subList(3, 6))
            } else {
                vizConfig = configVizOptions(args.subList(3, args.size()))
            }
        }

        return "Connection to Gephi - http://$host:$port/$workspace" + vizConfig
    }

    @Override
    Object configure(final List<String> args) throws RemoteException {
        if (args.size() != 2)
            throw new RemoteException("Expects [host <hostname>|port <port number>|workspace <name>|" +
                    "stepDelay <milliseconds>|startRGBColor <RGB array of floats>|" +
                    "colorToFade: <char r|g|b>]|colorFadeRate: <float>")

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
        else
            throw new RemoteException("Expects [host <hostname>|port <port number>|workspace <name>|" +
                    "stepDelay <milliseconds>|startRGBColor <RGB array of floats>|" +
                    "colorToFade: <char r|g|b>]|colorFadeRate: <float>")

        return "Connection to Gephi - http://$host:$port/$workspace" +
                " with stepDelay:$vizStepDelay, startRGBColor:$vizStartRGBColor, " +
                "colorToFade:$vizColorToFade, colorFadeRate:$vizColorFadeRate"
    }


    private Object configVizOptions(final List<String> vizConfigArgs) {
        if (vizConfigArgs.size() >= 1)
            parseVizStepDelay(vizConfigArgs[0])

        if (vizConfigArgs.size() >= 2)
            parseVizStartRGBColor(vizConfigArgs[1])

        if (vizConfigArgs.size() >= 3)
            parseVizColorToFade(vizConfigArgs[2])

        if (vizConfigArgs.size() >= 4)
            parseVizColorFadeRate(vizConfigArgs[3])


        return " with stepDelay:$vizStepDelay, startRGBColor:$vizStartRGBColor, " +
                "colorToFade:$vizColorToFade, colorFadeRate:$vizColorFadeRate"
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

    @Override
    Object submit(final List<String> args) throws RemoteException {
        final String line = String.join(" ", args)
        final Object o = shell.execute(line)
        if (o instanceof Graph) {
            clearGraph()
            def g = (Graph) o
            g.V().sideEffect { addVertexToGephi(it.get()) }.iterate()
        } else if (o instanceof Traversal) {
            fadingVertexColors = [:]
            def traversal = (Traversal) o
            def memKeys = traversal.asAdmin().getSideEffects().keys()
            def memSize = memKeys.size()
            // assumes user called store("1")...store("n") in ascension
            for (int i = 1; i <= memSize; i++) {
                def stepKey = Integer.toString(i)
                if (memKeys.contains(stepKey)) {
                    io.out.print("Visualizing vertices at step: $stepKey... ")
                    updateVisitedVertices()
                    int visitedCount = 0

                    if (traversal.asAdmin().getSideEffects().exists(stepKey)) {
                        traversal.asAdmin().getSideEffects().get(stepKey).each { element ->
                            visitVertexToGephi((Vertex) element)
                            visitedCount++
                        }
                    }
                    io.out.println("Visited: $visitedCount")
                }
                sleep(vizStepDelay)
            }
        }
    }

    @Override
    void close() throws IOException {

    }

    def updateVisitedVertices() {
        fadingVertexColors.keySet().each { vertex ->
            def currentColor = fadingVertexColors.get(vertex)
            currentColor *= vizColorFadeRate
            fadingVertexColors.put(vertex, currentColor)
            def props = [:]
            props.put(vizColorToFade.toString(), currentColor)
            updateGephiGraph([cn: [(vertex): props]])
        }
    }

    def visitVertexToGephi(def Vertex v) {
        def props = [:]
        props.put('r', vizStartRGBColor[0])
        props.put('g', vizStartRGBColor[1])
        props.put('b', vizStartRGBColor[2])
        props.put('x', 1)

        updateGephiGraph([cn: [(v.id().toString()): props]])

        fadingVertexColors.put(v.id().toString(), vizStartRGBColor[fadeColorIndex()])
    }

    def fadeColorIndex() {
        if (vizColorToFade == 'r')
            return 0
        else if (vizColorToFade == 'g')
            return 1
        else if (vizColorToFade == 'b')
            return 2
    }

    def addVertexToGephi(def Vertex v, def boolean ignoreEdges = false) {
        // grab the first property value from the strategies of values
        def props = v.valueMap().next().collectEntries { kv -> [(kv.key): kv.value[0]] }
        props << [label: v.label()]

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
        def props = e.valueMap().next()
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
