package com.nec;

import com.sun.jna.Library;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.nio.ByteBuffer;

public interface CountStringsLibrary extends Library {
    @Structure.FieldOrder({"data", "offsets", "count"})
    class varchar_vector extends Structure {
        public long data;
        public long offsets;
        public Integer count;

        public varchar_vector() {
            super();
        }

        public varchar_vector(Pointer p) {
            super(p);
            read();
        }
        public static class ByReference extends varchar_vector implements Structure.ByReference {
            public ByReference() { }
            public ByReference(Pointer p) { super(p); }
        }
    }

    @Structure.FieldOrder({"data", "offsets", "count"})
    class varchar_vector_raw extends Structure {
        public long data;
        public long offsets;
        public int count;

        public varchar_vector_raw() {
            super();
        }

        public varchar_vector_raw(Pointer p) {
            super(p);
            read();
        }
        public static class ByReference extends varchar_vector_raw implements Structure.ByReference {
            public ByReference() { }
            public ByReference(Pointer p) { super(p); }
        }
    }

    @Structure.FieldOrder({"data", "count"})
    class non_null_int_vector extends Structure {
        public Pointer data;
        public Integer count;

        public non_null_int_vector() {
            super();
        }

        public non_null_int_vector(Pointer p) {
            super(p);
            read();
        }
        public static class ByReference extends non_null_int_vector implements Structure.ByReference {
            public ByReference() { }
            public ByReference(Pointer p) { super(p); }
        }
    }

    @Structure.FieldOrder({"data", "count"})
    class non_null_double_vector extends Structure {
        public long data;
        public Integer count;

        public non_null_double_vector() {
            super();
        }

        public non_null_double_vector(int count) {
            this.count = count;
            super();
        }

        public non_null_double_vector(Pointer p) {
            super(p);
            read();
        }
        public static class ByReference extends non_null_double_vector implements Structure.ByReference {
            public ByReference() { }
            public ByReference(Pointer p) { super(p); }
        }
    }
}