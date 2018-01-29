package com.gzunk.streamsorter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StreamSortExampleApplication {

    private static final Logger LOG = LoggerFactory.getLogger(StreamSortExampleApplication.class);

    public static void main(String args[]) {
        LOG.info("Starting example application");

        try {
            StreamSorter streamSorter = new StreamSorter(new FileChunker());
            Path testFile = Paths.get("D:\\StreamSorter\\testdata2.csv");
            Path testDataSorted = Paths.get("D:\\StreamSorter\\testdatasorted.csv");

            try (PrintWriter output = new PrintWriter(testDataSorted.toFile())) {
                streamSorter.apply(Files.lines(testFile).map(SortableEntity::new))
                        .map(SortableEntity::unbuild).forEach(output::println);
            }

        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        LOG.info("Finished example application");
    }

}
