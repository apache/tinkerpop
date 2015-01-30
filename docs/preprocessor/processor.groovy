/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
import com.tinkerpop.gremlin.process.computer.util.ScriptEngineCache
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

import javax.script.ScriptContext

def BLOCK_DELIMITER = "----"
def RESULT_PREFIX = "==>"
def STATEMENT_CONTINUATION_CHARACTERS = [".", ",", "{", "("]
def STATEMENT_PREFIX = "gremlin> "
def STATEMENT_CONTINUATION_PREFIX = "         "

def header = """
    import com.tinkerpop.gremlin.process.graph.traversal.strategy.*
    import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.SocialTraversal
    import com.tinkerpop.gremlin.tinkergraph.structure.*
    import static java.util.Comparator.*
"""

def imports = new com.tinkerpop.gremlin.console.ConsoleImportCustomizerProvider()
def skipNextRead = false
def inCodeSection = false
def engine

sanitize = { def codeLine ->
    codeLine.replaceAll(/\s*(\<\d+\>\s*)*\<\d+\>\s*$/, "").replaceAll(/\s*\/\/.*$/, "").trim()
}

format = { def codeLine ->
    codeLine.replaceAll(/\s*((\s\<\d+\>\s*)*\s\<\d+\>)\s*$/, '   ////$1')
}

new File(this.args[0]).withReader { reader ->
    while (skipNextRead || (line = reader.readLine()) != null) {
        skipNextRead = false
        if (inCodeSection) {
            inCodeSection = !line.equals(BLOCK_DELIMITER)
            if (inCodeSection) {
                def script = new StringBuilder(header.toString())
                imports.getCombinedImports().each { script.append("import ${it}\n") }
                imports.getCombinedStaticImports().each { script.append("import static ${it}\n") }
                def sanitizedLine = sanitize(line)
                script.append(sanitizedLine)
                println STATEMENT_PREFIX + format(line)
                if (!sanitizedLine.isEmpty() && sanitizedLine[-1] in STATEMENT_CONTINUATION_CHARACTERS) {
                    while (true) {
                        line = reader.readLine()
                        if (!line.startsWith(" ") && !line.startsWith("}") && !line.startsWith(")") || line.equals(BLOCK_DELIMITER)) {
                            skipNextRead = true
                            break
                        }
                        sanitizedLine = sanitize(line)
                        script.append(sanitizedLine)
                        println STATEMENT_CONTINUATION_PREFIX + format(line)
                    }
                }
                if (line.startsWith("import ")) {
                    println "..."
                } else {
                    if (line.equals(BLOCK_DELIMITER)) {
                        skipNextRead = false
                        inCodeSection = false
                    }
                    def res
                    try {
                       res = engine.eval(script.toString())
                    } catch (e) {
                       e.printStackTrace()
                       System.exit(1)
                    }
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
                }
            }
            if (!inCodeSection) println BLOCK_DELIMITER
        } else {
            if (line.startsWith("[gremlin-")) {
                def parts = line.split(/,/, 2)
                def graph = parts.size() == 2 ? parts[1].capitalize().replaceAll(/\s*\]\s*$/, "") : ""
                def lang = parts[0].split(/-/, 2)[1].replaceAll(/\s*\]\s*$/, "")
                def g = graph.isEmpty() ? TinkerGraph.open() : TinkerFactory."create${graph}"()
                engine = ScriptEngineCache.get(lang)
                engine.put("g", g)
                engine.put("marko", g.V().has("name", "marko").tryNext().orElse(null))
                reader.readLine()
                inCodeSection = true
                println "[source,${lang}]"
                println BLOCK_DELIMITER
            } else println line
        }
    }
}
