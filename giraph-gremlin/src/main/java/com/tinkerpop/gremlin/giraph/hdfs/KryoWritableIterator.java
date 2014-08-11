package com.tinkerpop.gremlin.giraph.hdfs;

import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KryoWritableIterator implements Iterator<Pair> {

    private final KryoWritable key = new KryoWritable();
    private final KryoWritable value = new KryoWritable();
    private boolean available = false;
    private final Queue<SequenceFile.Reader> readers = new LinkedList<>();

    public KryoWritableIterator(final Configuration configuration, final Path path) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        for (final FileStatus status : fs.listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(new SequenceFile.Reader(fs, status.getPath(), configuration));
        }
    }

    public boolean hasNext() {
        try {
            if (this.available) {
                return true;
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        return false;
                    if (this.readers.peek().next(this.key, this.value)) {
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

    public Pair next() {
        try {
            if (this.available) {
                this.available = false;
                return new Pair(this.key.get(), this.value.get());
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        throw FastNoSuchElementException.instance();
                    if (this.readers.peek().next(this.key, this.value)) {
                        return new Pair(this.key.get(), this.value.get());
                    } else
                        this.readers.remove();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}