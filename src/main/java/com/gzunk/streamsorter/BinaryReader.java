package com.gzunk.streamsorter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.function.Consumer;

public class BinaryReader {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryReader.class);
    private final ByteBuffer keySizeBuffer = ByteBuffer.allocate(4);
    private final ByteBuffer dataSizeBuffer = ByteBuffer.allocate(4);
    private FileChannel fc;
    private byte[] key;
    private byte[] data;
    private boolean closed = false;
    private boolean copy; // Indicate whether the reader should create new arrays for each SortableEntity

    BinaryReader(Path input, int maxKeyLength, int maxDataLength, boolean copy) {
        try {
            LOG.debug("Opening channel for read");
            this.fc = FileChannel.open(input);
            this.key = new byte[maxKeyLength];
            this.data = new byte[maxDataLength];
            this.copy = copy;
        } catch (IOException e) {
            throw new RuntimeException("Unable to create input stream", e);
        }
    }

    private void close() {
        try {
            LOG.debug("Closing channel");
            fc.close();
            closed = true;
        } catch (IOException e) {
            throw new RuntimeException("Unable to close channel", e);
        }
    }

    public boolean tryAdvance(Consumer<? super SortableEntity> action) {

        if (closed) {
            return false;
        }

        try {
            LOG.trace("Reading Data");

            keySizeBuffer.rewind();
            int keySizeBytesRead = fc.read(keySizeBuffer);

            if (keySizeBytesRead == -1) {
                LOG.debug("End of File reached");
                close();
                return false;
            }

            dataSizeBuffer.rewind();
            int dataSizeBytesRead = fc.read(dataSizeBuffer);
            if (dataSizeBytesRead != Integer.BYTES) {
                throw new IOException("Unable to read data size");
            }

            // Convert bytes to ints
            keySizeBuffer.rewind();
            int keySize = keySizeBuffer.getInt();

            dataSizeBuffer.rewind();
            int dataSize = dataSizeBuffer.getInt();

            // Construct ByteBuffers of the appropriate size, wrapped around the byte arrays
            ByteBuffer keyBuffer = ByteBuffer.wrap(key, 0, keySize);
            ByteBuffer dataBuffer = ByteBuffer.wrap(data, 0, dataSize);

            // Now read the data into the byte arrays via the wrapper ByteBuffer
            int keyBufferBytesRead = fc.read(keyBuffer);
            if (keyBufferBytesRead != keySize) {
                throw new IOException("Key bytes read did not match key size");
            }

            int dataBufferBytesRead = fc.read(dataBuffer);
            if (dataBufferBytesRead != dataSize) {
                throw new IOException("Data bytes read did not match data size");
            }

            // Now build the object using the data populated in the arrays
            action.accept(new SortableEntity(keySize, key, dataSize, data, copy));
            return true;

        } catch (IOException e) {
            LOG.error("Exception while reading file: {}", e.getMessage());
            close();
            return false;
        }
    }
}
