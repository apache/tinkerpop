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

import java.nio.file.Files
import org.apache.tinkerpop.gremlin.driver.ser.*
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.structure.io.graphson.*
import org.apache.tinkerpop.gremlin.structure.io.Model
import org.apache.commons.configuration2.BaseConfiguration

new File("${projectBuildDir}/dev-docs/").mkdirs()
new File("${projectBuildDir}/test-case-data/io/graphson/").mkdirs()

conf = new BaseConfiguration()
conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name())
graph = TinkerGraph.open(conf)
TinkerFactory.generateTheCrew(graph)
g = graph.traversal()

model = Model.instance()

toJsonV1d0NoTypes = { o, type, mapper, comment = "" ->
    toJson(o, type, mapper, comment, "v1d0")
}

toJson = { o, type, mapper, comment = "", suffix = "" ->
    def jsonSample = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o)

    def fileToWriteTo = new File("${projectBuildDir}/test-case-data/io/graphson/" + type.toLowerCase().replace(" ","") + "-" + suffix + ".json")
    if (fileToWriteTo.exists()) fileToWriteTo.delete()
    fileToWriteTo.withWriter{ it.write(jsonSample) }

    return "==== " + type + "\n\n" +
            (comment.isEmpty() ? "" : comment + "\n\n") +
            "[source,json]\n" +
            "----\n" +
            jsonSample + "\n" +
            "----\n" +
            "\n"
}

writeSupportedV1Objects = { writer, mapper ->
    writer.write("=== Graph Structure\n\n")
    model.entries("Graph Structure").findAll{it.isCompatibleWith(GraphSONCompatibility.V1D0_3_4_0)}.each {
        writer.write(toJsonV1d0NoTypes(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }

    writer.write("\n")
    writer.write("=== RequestMessage\n\n")
    model.entries("RequestMessage").findAll{it.isCompatibleWith(GraphSONCompatibility.V1D0_3_4_0)}.each {
        writer.write(toJsonV1d0NoTypes(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }

    writer.write("\n")
    writer.write("=== ResponseMessage\n\n")
    model.entries("ResponseMessage").findAll{it.isCompatibleWith(GraphSONCompatibility.V1D0_3_4_0)}.each {
        writer.write(toJsonV1d0NoTypes(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }
}

mapper = GraphSONMapper.build().
        addRegistry(TinkerIoRegistryV1d0.instance()).
        addCustomModule(new AbstractGraphSONMessageSerializerV1d0.GremlinServerModule()).
        version(GraphSONVersion.V1_0).create().createMapper()

v1GraphSONFile = new File("${projectBuildDir}/dev-docs/out-graphson-1d0.txt")
if (v1GraphSONFile.exists()) v1GraphSONFile.delete()
new File("${projectBuildDir}/dev-docs/out-graphson-1d0.txt").withWriter { writeSupportedV1Objects(it, mapper) }

toJsonV2d0PartialTypes = { o, type, mapper, comment = "" ->
    toJson(o, type, mapper, comment, "v2d0-partial")
}

toJsonV2d0NoTypes = { o, type, mapper, comment = "" ->
    toJson(o, type, mapper, comment, "v2d0-no-types")
}

toJsonV3d0 = { o, type, mapper, comment = "" ->
    toJson(o, type, mapper, comment, "v3d0")
}

writeSupportedV2V3Objects = { writer, mapper, toJsonFunction, modelFilter, extendedText ->
    writer.write("=== Core\n\n")
    model.entries("Core").findAll(modelFilter).each {
        writer.write(toJsonFunction(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }

    writer.write("\n")
    writer.write("=== Graph Structure\n\n")
    model.entries("Graph Structure").findAll(modelFilter).each {
        writer.write(toJsonFunction(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }

    writer.write("\n")
    writer.write("=== Graph Process\n\n")
    model.entries("Graph Process").findAll(modelFilter).each {
        writer.write(toJsonFunction(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }

    writer.write("\n")
    writer.write("=== RequestMessage\n\n")
    model.entries("RequestMessage").findAll(modelFilter).each {
        writer.write(toJsonFunction(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }

    writer.write("\n")
    writer.write("=== ResponseMessage\n\n")
    model.entries("ResponseMessage").findAll(modelFilter).each {
        writer.write(toJsonFunction(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }

    writer.write("\n")
    writer.write("=== Extended\n\n")
    writer.write(extendedText)
    model.entries("Extended").findAll(modelFilter).each {
        writer.write(toJsonFunction(it.getObject(), it.getTitle(), mapper, it.getDescription()))
    }
}

mapper = GraphSONMapper.build().
        addRegistry(TinkerIoRegistryV2d0.instance()).
        typeInfo(TypeInfo.PARTIAL_TYPES).
        addCustomModule(GraphSONXModuleV2d0.build().create(false)).
        addCustomModule(new org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0.GremlinServerModule()).
        version(GraphSONVersion.V2_0).create().createMapper()

v2ExtendedDescription = """Note that the "extended" types require the addition of the separate `GraphSONXModuleV2d0` module as follows:

[source,java]
----
mapper = GraphSONMapper.build().
                        typeInfo(TypeInfo.PARTIAL_TYPES).
                        addCustomModule(GraphSONXModuleV2d0.build().create(false)).
                        version(GraphSONVersion.V2_0).create().createMapper()
----

"""

file = new File("${projectBuildDir}/dev-docs/out-graphson-2d0-partial.txt")
if (file.exists()) file.delete()
file.withWriter { writeSupportedV2V3Objects(it, mapper, toJsonV2d0PartialTypes, {it.isCompatibleWith(GraphSONCompatibility.V2D0_PARTIAL_3_5_0)}, v2ExtendedDescription) }

mapper = GraphSONMapper.build().
        addRegistry(TinkerIoRegistryV2d0.instance()).
        typeInfo(TypeInfo.NO_TYPES).
        addCustomModule(GraphSONXModuleV2d0.build().create(false)).
        addCustomModule(new org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0.GremlinServerModule()).
        version(GraphSONVersion.V2_0).create().createMapper()

file = new File("${projectBuildDir}/dev-docs/out-graphson-2d0-no-type.txt")
if (file.exists()) file.delete()
file.withWriter { writeSupportedV2V3Objects(it, mapper, toJsonV2d0NoTypes, {it.isCompatibleWith(GraphSONCompatibility.V2D0_NO_TYPE_3_5_0)}, v2ExtendedDescription) }

mapper = GraphSONMapper.build().
        addRegistry(TinkerIoRegistryV2d0.instance()).
        addCustomModule(GraphSONXModuleV2d0.build().create(false)).
        addCustomModule(new org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0.GremlinServerModule()).
        version(GraphSONVersion.V3_0).create().createMapper()

v3ExtendedDescription = """Note that the "extended" types require the addition of the separate `GraphSONXModuleV3d0` module as follows:

[source,java]
----
mapper = GraphSONMapper.build().
                        typeInfo(TypeInfo.PARTIAL_TYPES).
                        addCustomModule(GraphSONXModuleV3d0.build().create(false)).
                        version(GraphSONVersion.V3_0).create().createMapper()
----

"""

file = new File("${projectBuildDir}/dev-docs/out-graphson-3d0.txt")
if (file.exists()) file.delete()
file.withWriter { writeSupportedV2V3Objects(it, mapper, toJsonV3d0, {it.isCompatibleWith(GraphSONCompatibility.V3D0_PARTIAL_3_5_0)}, v3ExtendedDescription) }

def ver = "_" + "${projectVersion}".replace(".","_").replace("-SNAPSHOT","")
def target = "${projectBaseDir}/src/test/resources/org/apache/tinkerpop/gremlin/structure/io/graphson/" + ver
def targetDir = new File(target)
if (!targetDir.exists()) targetDir.mkdirs()
new File("${projectBuildDir}/test-case-data/io/graphson/").listFiles().each {
    def copyTo = new File(target + "/" + it.name)
    if (copyTo.exists()) copyTo.delete()
    Files.copy(it.toPath(), new File(target + "/" + it.name).toPath())
}
