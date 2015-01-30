package com.tinkerpop.gremlin.hadoop.structure.hdfs;

import com.tinkerpop.gremlin.process.FastNoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TextIterator implements Iterator<String> {

    private String line;
    private boolean available = false;
    private final Queue<BufferedReader> readers = new LinkedList<>();

    public TextIterator(final Configuration configuration, final Path path) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        for (final FileStatus status : fs.listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(new BufferedReader(new InputStreamReader(fs.open(status.getPath()))));
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (this.available) {
                return true;
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        return false;
                    if ((this.line = this.readers.peek().readLine()) != null) {
                        this.available = true;
                        return true;
                    } else
                        this.readers.remove();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String next() {
        try {
            if (this.available) {
                this.available = false;
                return this.line;
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        throw FastNoSuchElementException.instance();
                    if ((this.line = this.readers.peek().readLine()) != null) {
                        return this.line;
                    } else
                        this.readers.remove();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
