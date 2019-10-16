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

import io.netty.buffer.ByteBufAllocator
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter
import org.apache.tinkerpop.gremlin.driver.ser.NettyBufferFactory
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.structure.io.*
import org.apache.commons.configuration.BaseConfiguration

new File("${projectBuildDir}/test-case-data/io/graphbinary").mkdirs()

conf = new BaseConfiguration()
conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name())
graph = TinkerGraph.open(conf)
TinkerFactory.generateTheCrew(graph)
g = graph.traversal()

model = Model.instance()

allocator = ByteBufAllocator.DEFAULT
factory = new NettyBufferFactory()

toGraphBinary = { o, type, mapper, suffix = "" ->
    def fileToWriteTo = new File("${projectBuildDir}/test-case-data/io/graphbinary/" + type.title.toLowerCase().replace(" ","") + "-" + suffix + ".gbin")
    if (fileToWriteTo.exists()) fileToWriteTo.delete()
    filestream = new FileOutputStream(fileToWriteTo)
    try {
        buffer = factory.create(allocator.buffer())
        writer.write(o, buffer)
        buffer.readerIndex(0)
        buffer.readBytes(filestream, buffer.readableBytes())
    } catch (Exception ex) {
        if (ex.message == "Serializer for type org.apache.tinkerpop.gremlin.driver.message.RequestMessage not found" ||
                ex.message == "Serializer for type org.apache.tinkerpop.gremlin.driver.message.ResponseMessage not found" )
            fileToWriteTo.delete()
        else
            throw ex
    } finally {
        filestream.close()
        buffer.release()
    }
}

toGraphBinaryV1d0 = { o, type, mapper ->
    toGraphBinary(o, type, mapper, "v1")
}

writeSupportedObjects = { mapper, toGraphBinaryFunction ->
    model.entries().findAll{it.hasGraphBinaryCompatibility()}.each {
        toGraphBinaryFunction(it.getObject(), it, mapper)
    }
}

writer = new GraphBinaryWriter()

writeSupportedObjects(writer, toGraphBinaryV1d0)

def ver = "_" + "${projectVersion}".replace(".","_").replace("-SNAPSHOT","")
def target = "${projectBaseDir}/src/test/resources/org/apache/tinkerpop/gremlin/structure/io/graphbinary/" + ver
def targetDir = new File(target)
if (!targetDir.exists()) targetDir.mkdirs()
new File("${projectBuildDir}/test-case-data/io/graphbinary/").listFiles().each {
    def copyTo = new File(target + "/" + it.name)
    if (copyTo.exists()) copyTo.delete()
    java.nio.file.Files.copy(it.toPath(), new File(target + "/" + it.name).toPath())
}
