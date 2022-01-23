package io.mappedbus;

public interface SharedMemory {
    void unmap() throws Exception;
    long getLong(long pos);
    long getLongVolatile(long pos);
    void putLong(long pos, long val);
    void putLongVolatile(long pos, long val);
    long addr();
}
