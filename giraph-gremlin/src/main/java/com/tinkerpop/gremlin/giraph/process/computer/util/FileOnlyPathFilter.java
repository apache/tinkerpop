package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FileOnlyPathFilter implements Configurable, PathFilter {

    private Configuration configuration;
    private FileSystem fileSystem;

    public boolean accept(final Path path) {
        try {
            return path.toString().endsWith(this.configuration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION)) || this.fileSystem.isFile(path);
        } catch (Exception e) {
            return true;
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
        if (null != configuration) {
            try {
                fileSystem = FileSystem.get(this.configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }
}
