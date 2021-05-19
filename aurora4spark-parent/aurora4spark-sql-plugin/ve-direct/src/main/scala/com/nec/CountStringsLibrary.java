package com.nec;

import com.sun.jna.Library;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;

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
    }

    int add(int a, int b);

    int add_nums(int[] nums, int len);

    int count_strings(ByteBuffer strings, int[] string_positions,int[] string_lengths, int strings_count, PointerByReference results);
}