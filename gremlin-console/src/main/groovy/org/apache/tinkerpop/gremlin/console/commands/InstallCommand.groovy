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
package org.apache.tinkerpop.gremlin.console.commands

import org.apache.tinkerpop.gremlin.console.Mediator
import org.apache.tinkerpop.gremlin.console.plugin.PluggedIn
import org.apache.tinkerpop.gremlin.groovy.plugin.Artifact
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin
import groovy.grape.Grape
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

import java.nio.file.*
import java.util.jar.JarFile
import java.util.jar.Manifest

/**
 * Install a dependency into the console.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class InstallCommand extends CommandSupport {

    private final static String fileSep = System.getProperty("file.separator")
    private final Mediator mediator

    public InstallCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":install", ":+")
        this.mediator = mediator
    }

    @Override
    def Object execute(final List<String> arguments) {
        final def artifact = createArtifact(arguments)
        final def dep = makeDepsMap(artifact)
        final def pluginsThatNeedRestart = grabDeps(dep)

        final String extClassPath = getPathFromDependency(dep)
        final String extLibPath = extClassPath + fileSep + "lib"
        final String extPluginPath = extClassPath + fileSep + "plugin"
        final File f = new File(extClassPath)
        if (f.exists())
            return "A module with the name ${dep.module} is already installed"
        else {
            f.mkdirs()
            new File(extLibPath).mkdirs()
            new File(extPluginPath).mkdirs()
            new File(extClassPath + fileSep + "plugin-info.txt").withWriter { out -> out << arguments.join(":") }
        }

        final def dependencyLocations = Grape.resolve([classLoader: shell.getInterp().getClassLoader()], null, dep)

        def fs = FileSystems.default
        def targetPluginPath = fs.getPath(extPluginPath)
        def targetLibPath = fs.getPath(extLibPath)

        // collect the files already on the path in /lib. making some unfortunate assumptions about what the path
        // looks like for the gremlin distribution
        def filesAlreadyInPath = []
        def libClassPath
        try {
            libClassPath = fs.getPath(System.getProperty("user.dir") + fileSep + "lib")
            getFileNames(filesAlreadyInPath, libClassPath)
        } catch (Exception ignored) {
            // the user might have a non-standard directory system.  if they are non-standard then they must be
            // smart and they are therefore capable of resolving their own dependency problems.  this could also
            // mean that they are running gremlin from source and not from target/*standalone*
            io.println "Detected a non-standard Gremlin directory structure during install.  Expecting a 'lib' " +
                    "directory sibling to 'ext'. This message does not necessarily imply failure, however " +
                    "the console requires a certain directory structure for proper execution. Altering that " +
                    "structure can lead to unexpected behavior."
        }

        // for the "plugin" path ignore slf4j related jars.  they are already in the path and will create duplicate
        // bindings which generate annoying log messages that make you think stuff is wrong.  also, don't bring
        // over files that are already on the path
        dependencyLocations.collect { fs.getPath(it.path) }
                .findAll { !(it.fileName.toFile().name ==~ /(slf4j|logback\-classic)-.*\.jar/) }
                .findAll { !filesAlreadyInPath.collect { it.getFileName().toString() }.contains(it.fileName.toFile().name) }
                .each { Files.copy(it, targetPluginPath.resolve(it.fileName), StandardCopyOption.REPLACE_EXISTING) }

        // for the "lib" path include all dependencies
        dependencyLocations.collect { fs.getPath(it.path) }
                .each { Files.copy(it, targetLibPath.resolve(it.fileName), StandardCopyOption.REPLACE_EXISTING) }

        // additional dependencies are outside those pulled by grape and are defined in the manifest of the plugin jar.
        // if a plugin uses that setting, it should force "restart" when the plugin is activated.  right now,
        // it is up to the plugin developer to enforce that setting.
        getAdditionalDependencies(targetPluginPath, artifact).collect { fs.getPath(it.path) }
                .findAll { !(it.fileName.toFile().name ==~ /(slf4j|logback\-classic)-.*\.jar/) }
                .findAll { !filesAlreadyInPath.collect { it.getFileName().toString() }.contains(it.fileName.toFile().name)}
                .each { Files.copy(it, targetPluginPath.resolve(it.fileName), StandardCopyOption.REPLACE_EXISTING) }

        // for the "lib" path include all dependencies
        getAdditionalDependencies(targetLibPath, artifact).collect { fs.getPath(it.path) }
                .each { Files.copy(it, targetLibPath.resolve(it.fileName), StandardCopyOption.REPLACE_EXISTING) }

        // the ordering of jars seems to matter in some cases (e.g. neo4j).  the plugin system allows the plugin
        // to place a Gremlin-Plugin entry in the jar manifest file to define where specific jar files should
        // go in the path which provides enough flexibility to control when jars should load.  unfortunately,
        // this "ordering" issue doesn't seem to be documented as an issue anywhere and it is difficult to say
        // whether it is a java issue, groovy classloader issue, grape issue, etc.  see this issue for more
        // on the weirdness: https://github.org/apache/tinkerpop/tinkerpop3/issues/230
        //
        // another unfortunate side-effect to this approach is that manual cleanup of jars is kinda messy now
        // because you can't just delete the plugin director as one or more of the jars might have been moved.
        // unsure of what the long term effects of this is.  at the end of the day, users may simply need to
        // know something about their dependencies in order to have lots of "installed" plugins/dependencies.
        alterPaths(targetPluginPath, artifact)

        return "Loaded: " + arguments + (pluginsThatNeedRestart.size() == 0 ? "" : " - restart the console to use $pluginsThatNeedRestart")
    }

    private static String getPathFromDependency(final Map<String, Object> dep) {
        return System.getProperty("user.dir") + fileSep + "ext" + fileSep + (String) dep.module
    }

    private static alterPaths(final Path extPath, final Artifact artifact) {
        try {
            // another assumption about the pathing - seems safe for right now as the :install command is
            // responsible for all this stuff.  if the user chooses to manually install their dependencies
            // to the console, then it's up to them to sort this stuff out.
            def pathToInstalled = extPath.resolve(artifact.artifact + "-" + artifact.version + ".jar")
            final JarFile jar = new JarFile(pathToInstalled.toFile());
            final Manifest manifest = jar.getManifest()

            // containsKey doesn't seem to want to work - so just check for null - dah
            def attrLine = manifest.mainAttributes.getValue("Gremlin-Plugin-Paths")
            if (attrLine != null) {
                def splitLine = attrLine.split(";")
                splitLine.each {
                    def kv = it.split("=")
                    Files.move(extPath.resolve(kv[0]), extPath.resolve(kv[1]), StandardCopyOption.REPLACE_EXISTING)
                }
            }
        } catch (Exception ex) {
            // errors here will likely have to do with bad pathing or poorly constructed entries in the manifest.
            // hopefully these will only occur for developers of plugins who need to make use of this function.
            // internally to tinkerpop this is a neo4j-only issue
            throw new RuntimeException(ex)
        }
    }

    private Set<URI> getAdditionalDependencies(final Path extPath, final Artifact artifact) {
        try {
            def pathToInstalled = extPath.resolve(artifact.artifact + "-" + artifact.version + ".jar")
            final JarFile jar = new JarFile(pathToInstalled.toFile())
            final Manifest manifest = jar.getManifest()
            def attrLine = manifest.mainAttributes.getValue("Gremlin-Plugin-Dependencies")
            def additionalDependencies = [] as Set<URI>
            if (attrLine != null) {
                def splitLine = attrLine.split(";")
                splitLine.each {
                    def artifactBits = it.split(":")
                    def additional = new Artifact(artifactBits[0], artifactBits[1], artifactBits[2])

                    final def additionalDep = makeDepsMap(additional)
                    additionalDependencies.addAll(Grape.resolve([classLoader: shell.getInterp().getClassLoader()], null, additionalDep))
                }
            }

            return additionalDependencies
        } catch (Exception ex) {
            throw new RuntimeException(ex)
        }
    }

    private def grabDeps(final Map<String, Object> map) {
        Grape.grab(map)

        def pluginsThatNeedRestart = [] as Set

        // note that the service loader utilized the classloader from the groovy shell as shell class are available
        // from within there given loading through Grape.
        ServiceLoader.load(GremlinPlugin.class, shell.getInterp().getClassLoader()).forEach { plugin ->
            if (!mediator.availablePlugins.containsKey(plugin.class.name)) {
                mediator.availablePlugins.put(plugin.class.name, new PluggedIn(plugin, shell, io, false))
                if (plugin.requireRestart())
                    pluginsThatNeedRestart << plugin.name
            }
        }

        return pluginsThatNeedRestart
    }

    private static def createArtifact(final List<String> arguments) {
        final String group = arguments.size() >= 1 ? arguments.get(0) : null
        final String module = arguments.size() >= 2 ? arguments.get(1) : null
        final String version = arguments.size() >= 3 ? arguments.get(2) : null

        if (group == null || group.isEmpty())
            throw new IllegalArgumentException("Group cannot be null or empty")

        if (module == null || module.isEmpty())
            throw new IllegalArgumentException("Module cannot be null or empty")

        if (version == null || version.isEmpty())
            throw new IllegalArgumentException("Version cannot be null or empty")

        return new Artifact(group, module, version)
    }

    private def makeDepsMap(final Artifact artifact) {
        final Map<String, Object> map = new HashMap<>()
        map.put("classLoader", shell.getInterp().getClassLoader())
        map.put("group", artifact.getGroup())
        map.put("module", artifact.getArtifact())
        map.put("version", artifact.getVersion())
        map.put("changing", false)
        return map
    }

    private static void getFileNames(final List fileNames, final Path dir) {
        final DirectoryStream<Path> stream = Files.newDirectoryStream(dir)
        for (Path path : stream) {
            if (path.toFile().isDirectory()) getFileNames(fileNames, path)
            else {
                fileNames.add(path.toAbsolutePath())
            }
        }
        stream.close()
    }
}