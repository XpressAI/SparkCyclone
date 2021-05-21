package com.nec;

import com.sun.jna.Library;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.nio.ByteBuffer;

public interface CountStringsLibrary extends Library {
    @Structure.FieldOrder({"string_i", "count"})
    class unique_position_counter extends Structure {
        public int string_i;
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
    @Structure.FieldOrder({"data", "logical_total", "bytes_total"})
    class data_out extends Structure {
        public Pointer data;
        public long logical_total;
        public long bytes_total;

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

    int count_strings(ByteBuffer strings, int[] string_positions,int[] string_lengths, int strings_count, data_out.ByReference results);
}