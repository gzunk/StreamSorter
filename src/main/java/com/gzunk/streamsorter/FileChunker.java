package com.gzunk.streamsorter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.gzunk.streamsorter.Utility.createTempPath;

public class FileChunker implements Consumer<SortableEntity> {

    public static final Logger LOG = LoggerFactory.getLogger(FileChunker.class);
    public static final int DEFAULT_CHUNK_SIZE = 75000;
    private final int chunkSize;
    private final BinaryWriter bw = new BinaryWriter();
    private final List<Path> files = new ArrayList<>();
    private long count = 0;
    private int maxKeyLength = 0;
    private int maxDataLength = 0;

    public FileChunker() {
        this(DEFAULT_CHUNK_SIZE);
    }

    public FileChunker(int chunkSize) {
        this.chunkSize = chunkSize;
        LOG.info("Chunking files", chunkSize);
    }

    public List<Path> getFiles() {
        return files;
    }

    public int getMaxKeyLength() {
        return maxKeyLength;
    }

    public int getMaxDataLength() {
        return maxDataLength;
    }

    @Override
    public void accept(SortableEntity sortableEntity) {

        if (count % chunkSize == 0) {
            Path partial = createTempPath("partial");
            LOG.debug("Chunking to file: {}", partial);
            bw.newFile(partial);
            files.add(partial);
        }

        int keyLength = sortableEntity.getKeyLength();
        int dataLength = sortableEntity.getDataLength();

        maxKeyLength = keyLength > maxKeyLength ? keyLength : maxDataLength;
        maxDataLength = dataLength > maxDataLength ? dataLength : maxDataLength;

        bw.write(sortableEntity);
        count++;
    }

    public void close() {
        if (bw != null) {
            bw.close();
        }
    }
}
