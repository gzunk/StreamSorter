package com.gzunk.streamsorter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Random;

public class Utility {

    public static final Logger LOG = LoggerFactory.getLogger(Utility.class);

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String args[]) {

        String filename = args[0];
        long lines = Long.parseLong(args[1]);

        LOG.info("Filename: {}", filename);
        LOG.info("Lines passed: {}", lines);

        generateHugeFile(filename, lines);

    }

    private Utility() {
        // Private constructor to prevent instantiation
    }

    public static Path createTempPath(String prefix) {
        try {
            Path tempFile = Files.createTempFile(Paths.get("D:\\StreamSorter"), prefix, ".tmp");
            tempFile.toFile().deleteOnExit();
            return tempFile;
        } catch (IOException e) {
            throw new RuntimeException("Unable to create temp file", e);
        }
    }

    public static Path getFileFromResource(String resourceName) {
        try {
            Path tmpDir = Files.createTempDirectory("streamsorter");
            Path source = Paths.get(ClassLoader.getSystemResource(resourceName).toURI());
            Path target = tmpDir.resolve(source.getFileName());
            Files.copy(source, target);
            target.toFile().deleteOnExit();
            tmpDir.toFile().deleteOnExit();
            return target;
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Unable to create File from Resource", e);
        }
    }

    private static String generateString(Random rng) {
        int length = 80;

        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = ALPHABET.charAt(rng.nextInt(ALPHABET.length()));
        }

        return new String(text);
    }

    private static class GenerateLines implements Iterator<String> {

        Random rng = new Random();
        long count = 0;
        long lines = 0;

        public GenerateLines(long lines) {
            this.lines = lines;
        }

        @Override
        public boolean hasNext() {
            return count < lines;
        }

        @Override
        public String next() {
            count++;
            return generateString(rng);
        }
    }

    public static Path generateHugeFile(String filename, long lines) {

        Iterator<String> generateLines = new GenerateLines(lines);
        Iterable<String> generator = () -> generateLines;

        try {
            Path p = Files.createFile(Paths.get(filename));
            Files.write(p, generator, Charset.defaultCharset());
            return p;
        } catch (IOException e) {
            throw new RuntimeException("Unable to create test file");
        }
    }
}
