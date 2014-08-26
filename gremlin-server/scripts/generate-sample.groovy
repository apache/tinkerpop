// Generates a graph into the "empty" graph using the graph generators. "g" refers to the empty graph configured in the standard yaml configuration
(0..<10000).each { g.addVertex("oid", it) }
DistributionGenerator.build(g).label("knows").seedGenerator {
    987654321l
}.outDistribution(new PowerLawDistribution(2.1)).inDistribution(new PowerLawDistribution(2.1)).expectedNumEdges(100000).create().generate();
