/**
 * @author Daniel Kuppitz (http://thinkaurelius.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */

describeGraph = { Class<? extends com.tinkerpop.gremlin.structure.Graph> c ->
    def lf = System.getProperty("line.separator")
    def optIns = c.getAnnotationsByType(com.tinkerpop.gremlin.structure.Graph.OptIn)
    def optOuts = c.getAnnotationsByType(com.tinkerpop.gremlin.structure.Graph.OptOut)

    def optInCount = optIns != null ? optIns.size() : 0
    def optOutCount = optOuts != null ? optOuts.size() : 0
    def suitesSupported = optIns != null && optIns.size() > 0 ? optIns.collect { "> " + it.value() }.join(lf) : "> none"
    def testsOptedOut = optOuts != null && optOuts.size() > 0 ? optOuts.collect { "> " + it.test() + "#" + it.method() + "${lf}\t\"" + it.reason() + "\"" }.join(lf) : "> none";

    // not the use of {lf} here rather than triple quoted string is that groovy 2.4.0 seems to have trouble
    // parsing that into groovysh
    return "${lf}" +
"IMPLEMENTATION - ${c.getCanonicalName()} ${lf}" +
"TINKERPOP TEST SUITE ${lf}" +
"- Compliant with ($optInCount of 8 suites) + ${lf}" +
"$suitesSupported ${lf}" +
"- Opts out of $optOutCount individual tests ${lf}" +
"$testsOptedOut ${lf}" +
"- NOTE - ${lf}" +
"The describeGraph() function shows information about a Graph implementation. ${lf}" +
"It uses information found in Java Annotations on the implementation itself to ${lf}" +
"determine this output and does not assess the actual code of the test cases of ${lf}" +
"the implementation itself.  Compliant implementations will faithfully and ${lf}" +
"honestly supply these Annotations to provide the most accurate depiction of ${lf}" +
"their support."
}
