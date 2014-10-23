#!/bin/bash

# This script bumps version in the various files that reference the current TinkerPop version files (e.g. pom.xml)
# in preparation for release. Usage:
#
# bin/bump.sh "version"

# pom.xml - match the first occurrence of <version>*</version> after line X and replace
sed '11,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' pom.xml > pom1.xml && mv pom1.xml pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' giraph-gremlin/pom.xml > giraph-gremlin/pom1.xml && mv giraph-gremlin/pom1.xml giraph-gremlin/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' gremlin-algorithm/pom.xml > gremlin-algorithm/pom1.xml && mv gremlin-algorithm/pom1.xml gremlin-algorithm/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' gremlin-console/pom.xml > gremlin-console/pom1.xml && mv gremlin-console/pom1.xml gremlin-console/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' gremlin-core/pom.xml > gremlin-core/pom1.xml && mv gremlin-core/pom1.xml gremlin-core/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' gremlin-driver/pom.xml > gremlin-driver/pom1.xml && mv gremlin-driver/pom1.xml gremlin-driver/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' gremlin-groovy/pom.xml > gremlin-groovy/pom1.xml && mv gremlin-groovy/pom1.xml gremlin-groovy/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' gremlin-groovy-test/pom.xml > gremlin-groovy-test/pom1.xml && mv gremlin-groovy-test/pom1.xml gremlin-groovy-test/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' gremlin-server/pom.xml > gremlin-server/pom1.xml && mv gremlin-server/pom1.xml gremlin-server/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' gremlin-test/pom.xml > gremlin-test/pom1.xml && mv gremlin-test/pom1.xml gremlin-test/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' neo4j-gremlin/pom.xml > neo4j-gremlin/pom1.xml && mv neo4j-gremlin/pom1.xml neo4j-gremlin/pom.xml
sed '1,/<version>.*<\/version>/{s/<version>.*<\/version>/<version>'"$1"'<\/version>/}' tinkergraph-gremlin/pom.xml > tinkergraph-gremlin/pom1.xml && mv tinkergraph-gremlin/pom1.xml tinkergraph-gremlin/pom.xml

# YAML configuration
sed 's/\[com.tinkerpop, neo4j-gremlin, ".*"\]/\[com.tinkerpop, neo4j-gremlin, "'"$1"'"\]/' gremlin-server/conf/gremlin-server-neo4j.yaml > gremlin-server/conf/gremlin-server-neo4j1.yaml && mv gremlin-server/conf/gremlin-server-neo4j1.yaml gremlin-server/conf/gremlin-server-neo4j.yaml

# README
sed 's/\(http:\/\/tinkerpop.com\/.*docs\/\)[A-Za-z0-9.-]*\/\(.*\)/\1'"$1"'\/\2/' README.asciidoc > README1.asciidoc && mv README1.asciidoc README.asciidoc