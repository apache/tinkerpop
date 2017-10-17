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

/**
 * @author Daniel Kuppitz (http://thinkaurelius.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */

describeGraph = { Class<? extends org.apache.tinkerpop.gremlin.structure.Graph> c ->
    def lf = System.getProperty("line.separator")
    def optIns = c.getAnnotationsByType(org.apache.tinkerpop.gremlin.structure.Graph.OptIn)
    def optOuts = c.getAnnotationsByType(org.apache.tinkerpop.gremlin.structure.Graph.OptOut)

    def optInCount = optIns != null ? optIns.size() : 0
    def optOutCount = optOuts != null ? optOuts.size() : 0
    def suitesSupported = optIns != null && optIns.size() > 0 ? optIns.collect { "> " + it.value() }.join(lf) : "> none"
    def testsOptedOut = optOuts != null && optOuts.size() > 0 ? optOuts.collect { "> " + it.test() + "#" + it.method() + "${lf}\t\"" + it.reason() + "\"" }.join(lf) : "> none";

    // not the use of {lf} here rather than triple quoted string is that groovy 2.4.0 seems to have trouble
    // parsing that into groovysh - note the bug report here: https://issues.apache.org/jira/browse/GROOVY-7290
    return "${lf}" +
"IMPLEMENTATION - ${c.getCanonicalName()} ${lf}" +
"TINKERPOP TEST SUITE ${lf}" +
"- Compliant with ($optInCount of 11 suites) ${lf}" +
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
