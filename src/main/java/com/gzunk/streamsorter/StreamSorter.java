package com.gzunk.streamsorter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.gzunk.streamsorter.Utility.createTempPath;

public class StreamSorter implements UnaryOperator<Stream<SortableEntity>> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamSorter.class);
    private static final int SORT_CONCURRENCY = 16;
    private static final int MERGE_CONCURRENCY = 16;
    private final FileChunker chunker;

    StreamSorter(FileChunker chunker) {
        this.chunker = chunker;
    }

    private void writeStream(Stream<SortableEntity> input, Path output) {
        try (BinaryWriter writer = new BinaryWriter(output)) {
            input.forEach(writer);
        }
    }

    private Stream<SortableEntity> readStream(Path path) {
        return StreamSupport.stream(
                new BinarySpliterator(path, chunker.getMaxKeyLength(), chunker.getMaxDataLength()), false);
    }

    private void delete(Path fileToDelete) {
        try {
            Files.delete(fileToDelete);
        } catch (IOException e) {
            throw new RuntimeException("Unable to delete file", e);
        }
    }

    private Path sort(Path fileToSort) {
        LOG.debug("Sort Individual File: {}", fileToSort.toString());
        Path output = createTempPath("sorted");

        // Need to use the BinaryArrayBackedSpliterator because we're going to sort it
        Stream<SortableEntity> stream = StreamSupport.stream(
                new BinaryArrayBackedSpliterator(fileToSort, chunker.getMaxKeyLength(), chunker.getMaxDataLength()), false);

        writeStream(stream.sorted(), output);

        delete(fileToSort);
        return output;
    }

    private List<Path> sortFiles(List<Path> input) {
        LOG.info("Sorting Files");
        ForkJoinPool fjp = new ForkJoinPool(SORT_CONCURRENCY);
        try {
            return fjp.submit(() -> input.stream().parallel().map(this::sort).collect(Collectors.toList())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Path merge(Path first, Path second) {
        LOG.debug("Merging Two Files: {} {}", first, second);
        Path output = createTempPath("partialMerged");

        Stream<SortableEntity> merged = new MergingSpliterator(
                readStream(first).spliterator(), readStream(second).spliterator()).stream();
        writeStream(merged, output);

        delete(first);
        delete(second);
        return output;
    }

    private Path mergeFiles(List<Path> sorted) {
        LOG.info("Merging Files");
        ForkJoinPool fjp = new ForkJoinPool(MERGE_CONCURRENCY);
        try {
            return fjp.submit(() -> sorted.stream().parallel().reduce(this::merge)).get().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<SortableEntity> apply(Stream<SortableEntity> tStream) {

        // Split the input stream into multiple files
        tStream.forEach(chunker);
        List<Path> partials = chunker.getFiles();
        chunker.close();

        // Sort and Merge
        List<Path> sortedPartials = sortFiles(partials);
        Path merged = mergeFiles(sortedPartials);

        // And return the output stream
        return readStream(merged);
    }
}
