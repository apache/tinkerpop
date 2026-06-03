import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory

TinkerFactory.generateModern(modern)
TinkerFactory.generateClassic(classic)
TinkerFactory.generateTheCrew(crew)
TinkerFactory.generateGratefulDead(grateful)
TinkerFactory.generateKitchenSink(sink)

def globals = [:]
globals << [g       : traversal().withEmbedded(graph)]
globals << [ggraph  : traversal().withEmbedded(graph)]
globals << [gmodern : traversal().withEmbedded(modern)]
globals << [gclassic: traversal().withEmbedded(classic)]
globals << [gcrew   : traversal().withEmbedded(crew)]
globals << [ggrateful: traversal().withEmbedded(grateful)]
globals << [gsink   : traversal().withEmbedded(sink)]
