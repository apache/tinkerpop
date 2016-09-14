@Grab('com.xlson.groovycsv:groovycsv:1.1')
@Grab('org.apache.tinkerpop:tinkergraph-gremlin:3.2.2')
@Grab('org.slf4j:slf4j-log4j12:1.7.21')
import com.xlson.groovycsv.CsvParser
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.*
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.*
import org.apache.tinkerpop.gremlin.process.computer.traversal.*
import org.apache.tinkerpop.gremlin.util.function.*
import org.apache.tinkerpop.gremlin.structure.io.*
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.*
import groovy.sql.*
import org.apache.tinkerpop.gremlin.groovy.loaders.*
import groovy.json.*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.*
import org.apache.tinkerpop.gremlin.process.traversal.engine.*
import org.apache.tinkerpop.gremlin.groovy.jsr223.*
import org.apache.tinkerpop.gremlin.structure.io.gryo.*
import org.apache.tinkerpop.gremlin.process.remote.*
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.*
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.*
import org.apache.tinkerpop.gremlin.util.*
import org.apache.tinkerpop.gremlin.structure.util.*
import org.apache.tinkerpop.gremlin.structure.io.graphml.*
import org.apache.tinkerpop.gremlin.process.computer.*
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.*
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.*
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.*
import org.apache.tinkerpop.gremlin.structure.util.detached.*
import org.apache.tinkerpop.gremlin.structure.io.graphson.*
import org.apache.tinkerpop.gremlin.process.traversal.*
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.*
import org.apache.tinkerpop.gremlin.process.computer.bulkdumping.*
import org.apache.tinkerpop.gremlin.structure.util.empty.*
import org.apache.tinkerpop.gremlin.process.traversal.util.*
import org.apache.tinkerpop.gremlin.groovy.function.*
import static org.apache.tinkerpop.gremlin.process.traversal.SackFunctions.Barrier.*
import static org.apache.tinkerpop.gremlin.util.TimeUtil.*
import static org.apache.tinkerpop.gremlin.structure.Direction.*
import static org.apache.tinkerpop.gremlin.process.traversal.Pop.*
import static org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent.Pick.*
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.*
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.*
import static org.apache.tinkerpop.gremlin.process.traversal.P.*
import static org.apache.tinkerpop.gremlin.process.traversal.Order.*
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.*
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.*
import static org.apache.tinkerpop.gremlin.structure.Column.*
import static org.apache.tinkerpop.gremlin.process.computer.Computer.*
import static org.apache.tinkerpop.gremlin.structure.T.*
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.*
import org.apache.tinkerpop.gremlin.driver.ser.*
import org.apache.tinkerpop.gremlin.driver.message.*
import org.apache.tinkerpop.gremlin.driver.exception.*
import org.apache.tinkerpop.gremlin.driver.remote.*
import org.apache.tinkerpop.gremlin.driver.*
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.*
import java.security.SecureRandom


graph = TinkerGraph.open()
g = graph.traversal()

csv = new File("songs.csv").text

data = new CsvParser().parse(csv, readFirstLine:true, columnNames:['title', 'artist'])
for(line in data) {
   g.addV(label,"song", "title", line.title.toString(), "artist", line.artist).iterate()
}

// g.V().values('title').each{it -> it.split(' ').each{ x -> println x} }

new File("words.txt").each({ line ->
  g.addV(label, "noun", "word", line).iterate()
})

new File("numbers.txt").each({ line ->
  g.addV(label, "numbers", "value", line).iterate()
})

numbers = g.V().hasLabel("numbers").values('value').toList()

textContainsList = { List z -> new P({x, y -> 
  boolean hasIt = false
  y.each { v -> 
    if (x.contains(v))
      hasIt=true;
    }
  return hasIt; }, z)
}

words = g.V().hasLabel("noun").values('word').toList()

replaceNounWithGremlin = { title, nouns -> 
  results = []
  nouns.each { n ->
    if (title.contains(n)) {
      results << title.replace(n, "Gremlin")
    }
  }
  return results;
}

seed = new SecureRandom().generateSeed(3)
  
gremlins = g.V().hasLabel("song").has('title', textContainsList(numbers)).values('title').map({it -> replaceNounWithGremlin(it.get(), words)}).unfold().dedup().toList()
Collections.shuffle(gremlins, new SecureRandom(seed))
println gremlins.first()

