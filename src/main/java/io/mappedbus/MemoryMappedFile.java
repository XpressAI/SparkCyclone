/*
 * This class was inspired from an entry in Bryce Nyeggen's blog
 *
 * From MappedBus (Apache License 2.0)
 */
package io.mappedbus;

import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

/**
 * Class for direct access to a memory mapped file.
 */
@SuppressWarnings("restriction")
public class MemoryMappedFile implements SharedMemory {

    public static final Unsafe unsafe;
    private static final Method mmap;
    private static final Method unmmap;

    public final long addr;
    public final long size;
    public final String loc;

    static {
        try {
            Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singleoneInstanceField.setAccessible(true);
            unsafe = (Unsafe) singleoneInstanceField.get(null);
            mmap = FileChannelImpl.class.getDeclaredMethod( "map0", int.class, long.class, long.class);
            mmap.setAccessible(true);
            unmmap = FileChannelImpl.class.getDeclaredMethod("unmap0", long.class, long.class);
            unmmap.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static long roundTo4096(long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    public MemoryMappedFile(final String loc, long len) throws Exception {
        this.loc = loc;
        this.size = roundTo4096(len);
        try (final RandomAccessFile backingFile = new RandomAccessFile(this.loc, "rw")) {
            backingFile.setLength(this.size);
            final FileChannel ch = backingFile.getChannel();
            this.addr = (long) mmap.invoke(ch, 1, 0L, this.size);
            ch.close();
        }
    }

    public void unmap() throws Exception {
        unmmap.invoke(null, addr, this.size);
    }

    public long getLong(long pos) {
        return unsafe.getLong(pos + addr);
    }

    public long getLongVolatile(long pos) {
        return unsafe.getLongVolatile(null, pos + addr);
    }

    public void putLong(long pos, long val) {
        unsafe.putLong(pos + addr, val);
    }

    public void putLongVolatile(long pos, long val) {
        unsafe.putLongVolatile(null, pos + addr, val);
    }

    @Override
    public long addr() {
        return addr;
    }
}