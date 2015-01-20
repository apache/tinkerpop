import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

def header = """
    import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
    import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.SocialTraversal
    import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

    import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__
    import static com.tinkerpop.gremlin.process.T.*
    import static com.tinkerpop.gremlin.structure.Compare.*
    import static com.tinkerpop.gremlin.structure.Contains.*
    import static com.tinkerpop.gremlin.structure.Operator.*
    import static com.tinkerpop.gremlin.structure.Order.*
    import static java.util.Comparator.*
"""

def skipNextRead = false
def inCodeSection = false
def shell

new File(this.args[0]).withReader { reader ->
    while (skipNextRead || (line = reader.readLine()) != null) {
        skipNextRead = false
        if (inCodeSection) {
            if (line.equals("----")) inCodeSection = false
            if (inCodeSection) {
                def script = new StringBuilder(header)
                def lineNoRef = line.replaceAll(/\s*(\<\d+\>,\s*)*\<\d+\>\s*$/, "")
                def lineNoComment = lineNoRef.replaceAll(/\s*\/\/.*$/, "")
                script.append(lineNoComment)
                println "gremlin> " + line
                if (!line.isEmpty() && lineNoComment.toList().last() in [".", ",", "{", "("]) {
                    while (true) {
                        line = reader.readLine()
                        if (!line.startsWith(" ") && !line.startsWith("}") && !line.startsWith(")")) {
                            skipNextRead = true
                            break
                        }
                        lineNoRef = line.replaceAll(/\s*(\<\d+\>,\s*)*\<\d+\>\s*$/, "")
                        lineNoComment = lineNoRef.replaceAll(/\s*\/\/.*$/, "")
                        script.append(lineNoComment.trim())
                        println "         " + line // it should be "gremlin> ", but spaces make it copy/paste-friendly
                    }
                }
                def res = shell.evaluate(script.toString())
                if (!line.startsWith("import")) {
                    if (res instanceof Traversal) {
                        while (res.hasNext()) {
                            def current = res.next()
                            println "==>" + current
                        }
                    } else {
                        println "==>" + (res ?: "null")
                    }
                } else {
                    println "..."
                }
                if (line.equals("----")) {
                    skipNextRead = false
                    inCodeSection = false
                }
            }
            if (!inCodeSection) println line
        } else {
            if (line.startsWith("[gremlin")) {
                def parts = line.split(/,/, 2)
                def graph = parts.size() == 2 ? parts[1].capitalize().replaceAll(/\s*\]\s*$/, "") : ""
                def binding = new Binding()
                binding.setProperty("g", graph.isEmpty() ? TinkerGraph.open() : TinkerFactory."create${graph}"())
                shell = new GroovyShell(binding)
                reader.readLine()
                inCodeSection = true
                println "[source,groovy]\n----"
            } else println line
        }
    }
}
