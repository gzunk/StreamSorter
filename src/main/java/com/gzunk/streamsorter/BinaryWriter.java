package com.gzunk.streamsorter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

public class BinaryWriter implements Closeable, Consumer<SortableEntity> {

    public static final Logger LOG = LoggerFactory.getLogger(BinaryWriter.class);
    private FileChannel fc;

    public BinaryWriter() {
        // No args constructor for when we're chunking
    }

    public BinaryWriter(Path output) {
        newFile(output);
    }

    public void newFile(Path output) {

        // Close any existing file if it exists and is open
        if (fc != null && fc.isOpen()) {
            close();
        }

        try {
            LOG.debug("Opening channel for write");
            fc = FileChannel.open(output, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Unable to open channel");
        }
    }

    public long write(SortableEntity sortableEntity) {
        ByteBuffer[] buffer = sortableEntity.getBuffers();
        try {
            LOG.trace("Writing Data");
            return fc.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write data");
        }
    }

    @Override
    public void close() {
        try {
            LOG.debug("Closing channel");
            fc.close();
        } catch (IOException e) {
            throw new RuntimeException("Unable to close channel");
        }
    }

    @Override
    public void accept(SortableEntity sortableEntity) {
        write(sortableEntity);
    }

}
