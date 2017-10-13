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

import org.apache.tinkerpop.shaded.kryo.io.Output
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.structure.io.gryo.*
import org.apache.tinkerpop.gremlin.structure.io.*
import org.apache.commons.configuration.BaseConfiguration

new File("${projectBuildDir}/dev-docs/").mkdirs()
new File("${projectBuildDir}/test-case-data/io/gryo").mkdirs()

conf = new BaseConfiguration()
conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name())
graph = TinkerGraph.open(conf)
TinkerFactory.generateTheCrew(graph)
g = graph.traversal()

model = Model.instance()

toGryo = { o, type, mapper, suffix = "" ->
    def fileToWriteTo = new File("${projectBuildDir}/test-case-data/io/gryo/" + type.title.toLowerCase().replace(" ","") + "-" + suffix + ".kryo")
    if (fileToWriteTo.exists()) fileToWriteTo.delete()
    try {
        out = new Output(new FileOutputStream(fileToWriteTo))
        mapper.writeObject(out, o)
        out.close()
    } catch (Exception ex) {
        // some v1 gryo is not easily testable (i.e. RequestMessage) because it is has external serializers that
        // don' get registered with GryoMapper. those cases can be ignored.
        if (type.isCompatibleWith(GryoCompatibility.V1D0_3_3_0))
            throw ex
        else
            fileToWriteTo.delete()
    }
}

toGryoV1d0 = { o, type, mapper ->
    toGryo(o, type, mapper, "v1d0")
}

toGryoV3d0 = { o, type, mapper ->
    toGryo(o, type, mapper, "v3d0")
}

writeSupportedObjects = { mapper, toGryoFunction ->
    model.entries().findAll{it.hasGryoCompatibility()}.each {
        toGryoFunction(it.getObject(), it, mapper)
    }
}

mapper = GryoMapper.build().
        version(GryoVersion.V1_0).
        addRegistry(TinkerIoRegistryV2d0.instance()).
        create().createMapper()

writeSupportedObjects(mapper, toGryoV1d0)

mapper = GryoMapper.build().
        version(GryoVersion.V3_0).
        addRegistry(TinkerIoRegistryV2d0.instance()).
        create().createMapper()

writeSupportedObjects(mapper, toGryoV3d0)

def ver = "_" + "${projectVersion}".replace(".","_").replace("-SNAPSHOT","")
def target = "${projectBaseDir}/src/test/resources/org/apache/tinkerpop/gremlin/structure/io/gryo/" + ver
def targetDir = new File(target)
if (!targetDir.exists()) targetDir.mkdirs()
new File("${projectBuildDir}/test-case-data/io/gryo/").listFiles().each {
    def copyTo = new File(target + "/" + it.name)
    if (copyTo.exists()) copyTo.delete()
    java.nio.file.Files.copy(it.toPath(), new File(target + "/" + it.name).toPath())
}
