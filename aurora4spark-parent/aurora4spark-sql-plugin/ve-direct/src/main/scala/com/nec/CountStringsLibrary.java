package com.nec;

import com.sun.jna.Library;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.nio.ByteBuffer;

public interface CountStringsLibrary extends Library {
    @Structure.FieldOrder({"string_id", "count"})
    class unique_position_counter extends Structure {
        public int string_id;
        public int count;

        public unique_position_counter() {
        }

        public unique_position_counter(Pointer p) {
            super(p);
            read();
        }
        public static class ByReference extends data_out implements Structure.ByReference {
            public ByReference() { }
            public ByReference(Pointer p) { super(p); }
        }
    }

    @Structure.FieldOrder({"data", "count", "size"})
    class data_out extends Structure {
        public Pointer data;
        public long count;
        public long size;

        public data_out() {
        }

        public data_out(Pointer p) {
            super(p);
            read();
        }
        public static class ByReference extends data_out implements Structure.ByReference {
            public ByReference() { }
            public ByReference(Pointer p) { super(p); }
        }
    }

    @Structure.FieldOrder({"data", "offsets", "count"})
    class varchar_vector extends Structure {
        public Pointer data;
        public Pointer offsets;
        public long count;

        public varchar_vector() {
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
    @Structure.FieldOrder({"data", "count"})
    class non_null_int_vector extends Structure {
        public Pointer data;
        public long count;

        public long byteSize() {
            return count * 4;
        }

        public non_null_int_vector() {
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
}