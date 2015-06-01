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
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

def BLOCK_DELIMITER = "----"
def RESULT_PREFIX = "==>"
def STATEMENT_CONTINUATION_CHARACTERS = [".", ",", "{", "("]
def STATEMENT_PREFIX = "gremlin> "
def STATEMENT_CONTINUATION_PREFIX = "         "

def header = """
    import org.apache.tinkerpop.gremlin.tinkergraph.structure.*
"""

def imports = new org.apache.tinkerpop.gremlin.console.ConsoleImportCustomizerProvider()
def skipNextRead = false
def inCodeSection = false
def engine
def lineNumber = 0;
def line = "";
def plugins = []

sanitize = { def codeLine ->
    codeLine.replaceAll(/\s*(\<\d+\>\s*)*\<\d+\>\s*$/, "").replaceAll(/\s*\/\/.*$/, "").trim()
}

format = { def codeLine ->
    codeLine.replaceAll(/\s*((\s\<\d+\>\s*)*\s\<\d+\>)\s*$/, '   ////$1').replaceAll(/(\.\.\/){3}[^\/]+\//, "")
}

stringify = { def string ->
    "\"¶" + string.replaceAll("\\\\", "\\\\\\\\").replaceAll(/"/, "\\\\\"").replaceAll(/\$/, "\\\\\\\$") + "\""
}

//println "try {"
println stringify("START")

new File(this.args[0]).withReader { reader ->
    try {
        while (skipNextRead || (line = reader.readLine()) != null) {
            lineNumber++;
            skipNextRead = false
            if (inCodeSection) {
                inCodeSection = !line.equals(BLOCK_DELIMITER)
                if (inCodeSection) {
                    /*def script = new StringBuilder(header.toString())
                    imports.getCombinedImports().each { script.append("import ${it}\n") }
                    imports.getCombinedStaticImports().each { script.append("import static ${it}\n") }*/
                    def script = new StringBuilder()
                    def sanitizedLine = sanitize(line)
                    script.append(sanitizedLine)
                    println stringify(STATEMENT_PREFIX + format(line))
                    if (!sanitizedLine.isEmpty() && sanitizedLine[-1] in STATEMENT_CONTINUATION_CHARACTERS) {
                        while (true) {
                            line = reader.readLine()
                            if (!line.startsWith(" ") && !line.startsWith("}") && !line.startsWith(")") || line.equals(BLOCK_DELIMITER)) {
                                skipNextRead = true
                                break
                            }
                            sanitizedLine = sanitize(line)
                            script.append(sanitizedLine)
                            println stringify(STATEMENT_CONTINUATION_PREFIX + format(line))
                        }
                    }
                    if (line.startsWith("import ")) {
                        println "..."
                    } else {
                        if (line.equals(BLOCK_DELIMITER)) {
                            skipNextRead = false
                            inCodeSection = false
                        }
                        //def res = engine.eval(script.toString())
                        println script.toString()
/*
                        if (res instanceof Map) {
                            res = res.entrySet()
                        }
                        if (res instanceof Iterable) {
                            res = res.iterator()
                        }
                        if (res instanceof Iterator) {
                            while (res.hasNext()) {
                                def current = res.next()
                                println RESULT_PREFIX + (current ?: "null")
                            }
                        } else if (!line.isEmpty() && !line.startsWith("//")) {
                            println RESULT_PREFIX + (res ?: "null")
                        }
*/
                    }
                }
                if (!inCodeSection) {
                    println stringify(BLOCK_DELIMITER)
                }
            } else {
                if (line.startsWith("[gremlin-")) {
                    def parts = line.split(/,/, 2)
                    def graphString = parts.size() == 2 ? parts[1].capitalize().replaceAll(/\s*\]\s*$/, "") : ""
                    def lang = parts[0].split(/-/, 2)[1].replaceAll(/\s*\]\s*$/, "")
                    def graph = graphString.isEmpty() ? TinkerGraph.open() : TinkerFactory."create${graphString}"()
                    def g = graph.traversal(standard())
                    println stringify("IGNORE")
                    if (graphString.isEmpty()) {
                        println "graph = TinkerGraph.open()"
                    } else {
                        println "graph = TinkerFactory.create${graphString}()"
                    }
                    println "g = graph.traversal(standard())"
                    println "marko = g.V().has(\"name\", \"marko\").tryNext().orElse(null)"
                    println stringify("IGNORE")
                    reader.readLine()
                    inCodeSection = true
                    println stringify("[source,${lang}]")
                    println stringify(BLOCK_DELIMITER)
                } else println stringify(line)
            }
        }
    } catch (final Throwable e) {
        try {
            e.printStackTrace()
            throw new IllegalArgumentException("The script that failed:\n(${lineNumber}) ${line}");
        } catch (final Exception e1) {
            e1.printStackTrace();
            System.exit(1);
        }
    }
}

println "System.exit(0)"
//println "} catch (Exception e) {"
//println "  System.exit(1)"
//println "}"
