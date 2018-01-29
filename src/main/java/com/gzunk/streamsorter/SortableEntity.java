package com.gzunk.streamsorter;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SortableEntity implements Comparable<SortableEntity> {

    public static String unbuild(SortableEntity output) {
        return new String(output.data);
    }

    private String key;
    private byte[] data;
    private int dataSize;

    public SortableEntity(String input) {
        this.key  = input.substring(0, 10);
        this.data = input.getBytes();
        this.dataSize = this.data.length;
    }

    public SortableEntity(int keySize, byte[] key, int dataSize, byte[] data, boolean copy) {
        this.key = new String(key, 0, keySize);
        this.data = copy ? Arrays.copyOf(data, dataSize) : data; // copy data if asked
        this.dataSize = dataSize;
    }

    @Override
    public int compareTo(SortableEntity o) {
        return this.key.compareTo(o.key);
    }

    public int getKeyLength() {
        return key.length();
    }

    public int getDataLength() {
        return data.length;
    }

    public ByteBuffer[] getBuffers() {

        ByteBuffer[] buffer = new ByteBuffer[4];

        buffer[0] = ByteBuffer.allocate(Integer.BYTES);
        buffer[0].putInt(key.length());
        buffer[0].rewind();
        buffer[1] = ByteBuffer.allocate(Integer.BYTES);
        buffer[1].putInt(dataSize);
        buffer[1].rewind();
        buffer[2] = ByteBuffer.wrap(key.getBytes());
        buffer[3] = ByteBuffer.wrap(data, 0, dataSize);

        return buffer;
    }
}
