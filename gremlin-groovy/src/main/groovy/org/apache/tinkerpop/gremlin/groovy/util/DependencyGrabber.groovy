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
package org.apache.tinkerpop.gremlin.groovy.util

import groovy.grape.Grape
import org.apache.commons.lang3.SystemUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.DirectoryStream
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.jar.JarFile
import java.util.jar.Manifest

/**
 * This class provides a way to copy an {@link Artifact} and it's dependencies from Maven repositories down to
 * the local system.  This capability is useful for the {@code :install} command in Gremlin Console and for the
 * {@code -i} option on {@code gremlin-server.sh}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class DependencyGrabber {
    private static final Logger logger = LoggerFactory.getLogger(DependencyGrabber.class);

    private final static String fileSep = System.getProperty("file.separator")
    private final ClassLoader classLoaderToUse
    private final String extensionDirectory

    public DependencyGrabber(final ClassLoader cl, final String extensionDirectory) {
        this.classLoaderToUse = cl
        this.extensionDirectory = extensionDirectory
    }

    /**
     * @deprecated As of release 3.2.4, replaced by {@link #deleteDependenciesFromPath(Artifact)}
     */
    def String deleteDependenciesFromPath(final org.apache.tinkerpop.gremlin.groovy.plugin.Artifact artifact) {
        deleteDependenciesFromPath(new Artifact(artifact.group, artifact.artifact, artifact.version))
    }

    def String deleteDependenciesFromPath(final Artifact artifact) {
        final def dep = makeDepsMap(artifact)
        final String extClassPath = getPathFromDependency(dep)
        final File f = new File(extClassPath)
        if (!f.exists()) {
            return "There is no module with the name ${dep.module} to remove - ${extClassPath}"
        }
        else {
            f.deleteDir()
            return "Uninstalled ${dep.module}"
        }
    }

    /**
     * @deprecated As of release 3.2.4, replaced by {@link #copyDependenciesToPath(Artifact)}
     */
    def String copyDependenciesToPath(final org.apache.tinkerpop.gremlin.groovy.plugin.Artifact artifact) {
        copyDependenciesToPath(new Artifact(artifact.group, artifact.artifact, artifact.version))
    }

    def Set<String> copyDependenciesToPath(final Artifact artifact) {
        final def dep = makeDepsMap(artifact)
        final String extClassPath = getPathFromDependency(dep)
        final String extLibPath = extClassPath + fileSep + "lib"
        final String extPluginPath = extClassPath + fileSep + "plugin"
        final File f = new File(extClassPath)

        if (f.exists()) throw new IllegalStateException("a module with the name ${dep.module} is already installed")

        try {
            if (!f.mkdirs()) throw new IOException("could not create directory at ${f}")
            if (!new File(extLibPath).mkdirs()) throw new IOException("could not create directory at ${extLibPath}")
            if (!new File(extPluginPath).mkdirs()) throw new IOException("could not create directory at ${extPluginPath}")
        } catch (IOException ioe) {
            // installation failed. make sure to cleanup directories.
            deleteDependenciesFromPath(artifact)
            throw ioe
        }

        new File(extClassPath + fileSep + "plugin-info.txt").withWriter { out -> out << [artifact.group, artifact.artifact, artifact.version].join(":") }

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
            logger.warn("Detected a non-standard Gremlin directory structure during install.  Expecting a 'lib' " +
                    "directory sibling to 'ext'. This message does not necessarily imply failure, however " +
                    "the console requires a certain directory structure for proper execution. Altering that " +
                    "structure can lead to unexpected behavior.")
        }

        try {
            final def dependencyLocations = [] as Set<URI>
            dependencyLocations.addAll(Grape.resolve([classLoader: this.classLoaderToUse], null, dep))

            // for the "plugin" path ignore slf4j related jars.  they are already in the path and will create duplicate
            // bindings which generate annoying log messages that make you think stuff is wrong.  also, don't bring
            // over files that are already on the path. these dependencies will be part of the classpath
            //
            // additional dependencies are outside those pulled by grape and are defined in the manifest of the plugin jar.
            // if a plugin uses that setting, it should force "restart" when the plugin is activated.  right now,
            // it is up to the plugin developer to enforce that setting.
            dependencyLocations.collect(convertUriToPath(fs))
                    .findAll { !(it.fileName.toFile().name ==~ /(slf4j|logback\-classic)-.*\.jar/) }
                    .findAll {!filesAlreadyInPath.collect { it.getFileName().toString() }.contains(it.fileName.toFile().name)}
                    .each(copyTo(targetPluginPath))
            getAdditionalDependencies(targetPluginPath, artifact).collect(convertUriToPath(fs))
                    .findAll { !(it.fileName.toFile().name ==~ /(slf4j|logback\-classic)-.*\.jar/) }
                    .findAll { !filesAlreadyInPath.collect { it.getFileName().toString() }.contains(it.fileName.toFile().name)}
                    .each(copyTo(targetPluginPath))

            // get dependencies for the lib path.  the lib path should not filter out any jars - used for reference
            dependencyLocations.collect(convertUriToPath(fs)).each(copyTo(targetLibPath))
            getAdditionalDependencies(targetLibPath, artifact).collect(convertUriToPath(fs)).each(copyTo(targetLibPath))
        }
        catch (Exception e) {
            // installation failed. make sure to cleanup directories.
            deleteDependenciesFromPath(artifact)
            throw e
        }

        // the ordering of jars seems to matter in some cases (e.g. neo4j).  the plugin system allows the plugin
        // to place a Gremlin-Plugin-Paths entry in the jar manifest file to define where specific jar files should
        // go in the path which provides enough flexibility to control when jars should load.  unfortunately,
        // this "ordering" issue doesn't seem to be documented as an issue anywhere and it is difficult to say
        // whether it is a java issue, groovy classloader issue, grape issue, etc.  see this issue for more
        // on the weirdness: https://github.org/apache/tinkerpop/tinkerpop3/issues/230
        //
        // another unfortunate side-effect to this approach is that manual cleanup of jars is kinda messy now
        // because you can't just delete the plugin directory as one or more of the jars might have been moved.
        // unsure of what the long term effects of this is.  at the end of the day, users may simply need to
        // know something about their dependencies in order to have lots of "installed" plugins/dependencies.
        alterPaths("Gremlin-Plugin-Paths", targetPluginPath, artifact)
        alterPaths("Gremlin-Lib-Paths", targetLibPath, artifact)
    }

    private static Closure copyTo(final Path path) {
        return { Path p ->
            // check for existence prior to copying as windows systems seem to have problems with REPLACE_EXISTING
            def copying = path.resolve(p.fileName)
            if (!copying.toFile().exists()) {
                Files.copy(p, copying, StandardCopyOption.REPLACE_EXISTING)
                logger.info("Copying - $copying")
            }
        }
    }

    /**
     * Windows places a starting forward slash in the URI that needs to be stripped off or else the
     * {@code FileSystem} won't properly resolve it.
     */
    private static Closure convertUriToPath(def fs) {
        return { URI uri ->
            def p = SystemUtils.IS_OS_WINDOWS ? uri.path.substring(1) : uri.path
            return fs.getPath(p)
        }
    }

    private Set<URI> getAdditionalDependencies(final Path extPath, final Artifact artifact) {
        try {
            def pathToInstalled = extPath.resolve(artifact.artifact + "-" + artifact.version + ".jar")
            final JarFile jar = new JarFile(pathToInstalled.toFile())
            try {
                final Manifest manifest = jar.getManifest()
                def attrLine = manifest.mainAttributes.getValue("Gremlin-Plugin-Dependencies")
                def additionalDependencies = [] as Set<URI>
                if (attrLine != null) {
                    def splitLine = attrLine.split(";")
                    splitLine.each {
                        def artifactBits = it.split(":")
                        def additional = new Artifact(artifactBits[0], artifactBits[1], artifactBits[2])

                        final def additionalDep = makeDepsMap(additional)
                        additionalDependencies.addAll(Grape.resolve([classLoader: this.classLoaderToUse], null, additionalDep))
                    }
                }
                return additionalDependencies
            } finally {
                jar.close()
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex)
        }
    }

    private static alterPaths(final String manifestEntry, final Path extPath, final Artifact artifact) {
        try {
            def pathToInstalled = extPath.resolve(artifact.artifact + "-" + artifact.version + ".jar")
            final JarFile jar = new JarFile(pathToInstalled.toFile())
            try {
                final Manifest manifest = jar.getManifest()
                def attrLine = manifest.mainAttributes.getValue(manifestEntry)
                if (attrLine != null) {
                    def splitLine = attrLine.split(";")
                    splitLine.each {
                        if (it.endsWith("="))
                            Files.delete(extPath.resolve(it.substring(0, it.length() - 1)))
                        else {
                            def kv = it.split("=")
                            Files.move(extPath.resolve(kv[0]), extPath.resolve(kv[1]), StandardCopyOption.REPLACE_EXISTING)
                        }
                    }
                }
            } finally {
                jar.close()
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex)
        }
    }

    private String getPathFromDependency(final Map<String, Object> dep) {
        return this.extensionDirectory + fileSep + (String) dep.module
    }

    public def makeDepsMap(final Artifact artifact) {
        final Map<String, Object> map = new HashMap<>()
        map.put("classLoader", this.classLoaderToUse)
        map.put("group", artifact.getGroup())
        map.put("module", artifact.getArtifact())
        map.put("version", artifact.getVersion())
        map.put("changing", false)
        return map
    }

    public static void getFileNames(final List fileNames, final Path dir) {
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
