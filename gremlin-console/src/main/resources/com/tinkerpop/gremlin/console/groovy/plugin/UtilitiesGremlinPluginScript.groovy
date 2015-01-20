/**
 * @author Daniel Kuppitz (http://thinkaurelius.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */

clock = { int loops = 100, Closure closure ->
    closure.call() // warmup
    (1..loops).collect {
        t = System.nanoTime()
        closure.call()
        ((System.nanoTime() - t) * 0.000001)
    }.sum() / loops
}

describeGraph = { Class<? extends com.tinkerpop.gremlin.structure.Graph> c ->
    def lf = System.getProperty("line.separator")
    def optIns = c.getAnnotationsByType(com.tinkerpop.gremlin.structure.Graph.OptIn)
    def optOuts = c.getAnnotationsByType(com.tinkerpop.gremlin.structure.Graph.OptOut)

    def optInCount = optIns != null ? optIns.size() : 0
    def optOutCount = optOuts != null ? optOuts.size() : 0
    def suitesSupported = optIns != null && optIns.size() > 0 ? optIns.collect { "> " + it.value() }.join(lf) : "> none"
    def testsOptedOut = optOuts != null && optOuts.size() > 0 ? optOuts.collect {
        "> " + it.test() + "#" + it.method() + "${lf}\t\"" + it.reason() + "\""
    }.join(lf) : "> none"
    """
IMPLEMENTATION - ${c.getCanonicalName()}
TINKERPOP TEST SUITE
- Compliant with ($optInCount of 8 suites)
$suitesSupported
- Opts out of $optOutCount individual tests
$testsOptedOut${lf}
- NOTE -
The describeGraph() function shows information about a Graph implementation.
It uses information found in Java Annotations on the implementation itself to
determine this output and does not assess the actual code of the test cases of
the implementation itself.  Compliant implementations will faithfully and
honestly supply these Annotations to provide the most accurate depiction of
their support."""
}