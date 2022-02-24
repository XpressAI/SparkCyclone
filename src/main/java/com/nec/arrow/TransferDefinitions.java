// Targeted by JavaCPP version 1.5.6: DO NOT EDIT THIS FILE

package com.nec.arrow;

import java.nio.*;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class TransferDefinitions extends com.nec.arrow.TransferDefinitionsConfig {
    static { Loader.load(); }

@Name("std::vector<std::string>") public static class StringVector extends Pointer {
    static { Loader.load(); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public StringVector(Pointer p) { super(p); }
    public StringVector(BytePointer value) { this(1); put(0, value); }
    public StringVector(BytePointer ... array) { this(array.length); put(array); }
    public StringVector(String value) { this(1); put(0, value); }
    public StringVector(String ... array) { this(array.length); put(array); }
    public StringVector()       { allocate();  }
    public StringVector(long n) { allocate(n); }
    private native void allocate();
    private native void allocate(@Cast("size_t") long n);
    public native @Name("operator =") @ByRef StringVector put(@ByRef StringVector x);

    public boolean empty() { return size() == 0; }
    public native long size();
    public void clear() { resize(0); }
    public native void resize(@Cast("size_t") long n);

    @Index(function = "at") public native @StdString BytePointer get(@Cast("size_t") long i);
    public native StringVector put(@Cast("size_t") long i, BytePointer value);
    @ValueSetter @Index(function = "at") public native StringVector put(@Cast("size_t") long i, @StdString String value);

    public native @ByVal Iterator insert(@ByVal Iterator pos, @StdString BytePointer value);
    public native @ByVal Iterator erase(@ByVal Iterator pos);
    public native @ByVal Iterator begin();
    public native @ByVal Iterator end();
    @NoOffset @Name("iterator") public static class Iterator extends Pointer {
        public Iterator(Pointer p) { super(p); }
        public Iterator() { }

        public native @Name("operator ++") @ByRef Iterator increment();
        public native @Name("operator ==") boolean equals(@ByRef Iterator it);
        public native @Name("operator *") @StdString BytePointer get();
    }

    public BytePointer[] get() {
        BytePointer[] array = new BytePointer[size() < Integer.MAX_VALUE ? (int)size() : Integer.MAX_VALUE];
        for (int i = 0; i < array.length; i++) {
            array[i] = get(i);
        }
        return array;
    }
    @Override public String toString() {
        return java.util.Arrays.toString(get());
    }

    public BytePointer pop_back() {
        long size = size();
        BytePointer value = get(size - 1);
        resize(size - 1);
        return value;
    }
    public StringVector push_back(BytePointer value) {
        long size = size();
        resize(size + 1);
        return put(size, value);
    }
    public StringVector put(BytePointer value) {
        if (size() != 1) { resize(1); }
        return put(0, value);
    }
    public StringVector put(BytePointer ... array) {
        if (size() != array.length) { resize(array.length); }
        for (int i = 0; i < array.length; i++) {
            put(i, array[i]);
        }
        return this;
    }

    public StringVector push_back(String value) {
        long size = size();
        resize(size + 1);
        return put(size, value);
    }
    public StringVector put(String value) {
        if (size() != 1) { resize(1); }
        return put(0, value);
    }
    public StringVector put(String ... array) {
        if (size() != array.length) { resize(array.length); }
        for (int i = 0; i < array.length; i++) {
            put(i, array[i]);
        }
        return this;
    }
}

@Name("std::vector<size_t>") public static class SizeTVector extends Pointer {
    static { Loader.load(); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public SizeTVector(Pointer p) { super(p); }
    public SizeTVector(long ... array) { this(array.length); put(array); }
    public SizeTVector()       { allocate();  }
    public SizeTVector(long n) { allocate(n); }
    private native void allocate();
    private native void allocate(@Cast("size_t") long n);
    public native @Name("operator =") @ByRef SizeTVector put(@ByRef SizeTVector x);

    public boolean empty() { return size() == 0; }
    public native long size();
    public void clear() { resize(0); }
    public native void resize(@Cast("size_t") long n);

    @Index(function = "at") public native @Cast("size_t") long get(@Cast("size_t") long i);
    public native SizeTVector put(@Cast("size_t") long i, long value);

    public native @ByVal Iterator insert(@ByVal Iterator pos, @Cast("size_t") long value);
    public native @ByVal Iterator erase(@ByVal Iterator pos);
    public native @ByVal Iterator begin();
    public native @ByVal Iterator end();
    @NoOffset @Name("iterator") public static class Iterator extends Pointer {
        public Iterator(Pointer p) { super(p); }
        public Iterator() { }

        public native @Name("operator ++") @ByRef Iterator increment();
        public native @Name("operator ==") boolean equals(@ByRef Iterator it);
        public native @Name("operator *") @Cast("size_t") long get();
    }

    public long[] get() {
        long[] array = new long[size() < Integer.MAX_VALUE ? (int)size() : Integer.MAX_VALUE];
        for (int i = 0; i < array.length; i++) {
            array[i] = get(i);
        }
        return array;
    }
    @Override public String toString() {
        return java.util.Arrays.toString(get());
    }

    public long pop_back() {
        long size = size();
        long value = get(size - 1);
        resize(size - 1);
        return value;
    }
    public SizeTVector push_back(long value) {
        long size = size();
        resize(size + 1);
        return put(size, value);
    }
    public SizeTVector put(long value) {
        if (size() != 1) { resize(1); }
        return put(0, value);
    }
    public SizeTVector put(long ... array) {
        if (size() != array.length) { resize(array.length); }
        for (int i = 0; i < array.length; i++) {
            put(i, array[i]);
        }
        return this;
    }
}

// Parsed from transfer-definitions.hpp

/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
// #pragma once

// #ifndef VE_TD_DEFS

// #include <stdint.h>
// #include <type_traits>

// Explicitly instantiate struct template for int32_t
//typedef NullableScalarVec<int32_t> nullable_int_vector;

public static class nullable_int_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public nullable_int_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public nullable_int_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public nullable_int_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public nullable_int_vector position(long position) {
        return (nullable_int_vector)super.position(position);
    }
    @Override public nullable_int_vector getPointer(long i) {
        return new nullable_int_vector((Pointer)this).offsetAddress(i);
    }

  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  public native IntPointer data(); public native nullable_int_vector data(IntPointer setter);              // The raw data
  public native @Cast("uint64_t*") LongPointer validityBuffer(); public native nullable_int_vector validityBuffer(LongPointer setter);    // Bit vector to denote null values
  public native int count(); public native nullable_int_vector count(int setter);              // Row count (synonymous with size of data array)
  public int dataSize() {
        return count() * 4;
  }
  // Returns a deep copy of this NullableScalarVec<T>
  public native nullable_int_vector clone();

  // Value equality check against another NullableScalarVec<T>
  public native @Cast("bool") boolean equals(@Const nullable_int_vector other);
}

// Explicitly instantiate struct template for int64_t
//typedef NullableScalarVec<int64_t> nullable_bigint_vector;

public static class nullable_bigint_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public nullable_bigint_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public nullable_bigint_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public nullable_bigint_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public nullable_bigint_vector position(long position) {
        return (nullable_bigint_vector)super.position(position);
    }
    @Override public nullable_bigint_vector getPointer(long i) {
        return new nullable_bigint_vector((Pointer)this).offsetAddress(i);
    }

  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  public native @Cast("int64_t*") LongPointer data(); public native nullable_bigint_vector data(LongPointer setter);              // The raw data
  public native @Cast("uint64_t*") LongPointer validityBuffer(); public native nullable_bigint_vector validityBuffer(LongPointer setter);    // Bit vector to denote null values
  public native int count(); public native nullable_bigint_vector count(int setter);              // Row count (synonymous with size of data array)
  public int dataSize() {
        return count() * 8;
  }
  // Returns a deep copy of this NullableScalarVec<T>
  public native nullable_bigint_vector clone();

  // Value equality check against another NullableScalarVec<T>
  public native @Cast("bool") boolean equals(@Const nullable_bigint_vector other);
}

// Explicitly instantiate struct template for float
//typedef NullableScalarVec<float> nullable_float_vector;

public static class nullable_float_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public nullable_float_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public nullable_float_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public nullable_float_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public nullable_float_vector position(long position) {
        return (nullable_float_vector)super.position(position);
    }
    @Override public nullable_float_vector getPointer(long i) {
        return new nullable_float_vector((Pointer)this).offsetAddress(i);
    }

  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  public native FloatPointer data(); public native nullable_float_vector data(FloatPointer setter);              // The raw data
  public native @Cast("uint64_t*") LongPointer validityBuffer(); public native nullable_float_vector validityBuffer(LongPointer setter);    // Bit vector to denote null values
  public native int count(); public native nullable_float_vector count(int setter);              // Row count (synonymous with size of data array)

  public int dataSize() {
        return count() * 4;
  }

  // Returns a deep copy of this NullableScalarVec<T>
  public native nullable_float_vector clone();

  // Value equality check against another NullableScalarVec<T>
  public native @Cast("bool") boolean equals(@Const nullable_float_vector other);
}

// Explicitly instantiate struct template for double
//typedef NullableScalarVec<double> nullable_double_vector;

public static class nullable_double_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public nullable_double_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public nullable_double_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public nullable_double_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public nullable_double_vector position(long position) {
        return (nullable_double_vector)super.position(position);
    }
    @Override public nullable_double_vector getPointer(long i) {
        return new nullable_double_vector((Pointer)this).offsetAddress(i);
    }

  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  public native DoublePointer data(); public native nullable_double_vector data(DoublePointer setter);              // The raw data
  public native @Cast("uint64_t*") LongPointer validityBuffer(); public native nullable_double_vector validityBuffer(LongPointer setter);    // Bit vector to denote null values
  public native int count(); public native nullable_double_vector count(int setter);              // Row count (synonymous with size of data array)
  public int dataSize() {
        return count() * 8;
  }
  // Returns a deep copy of this NullableScalarVec<T>
  public native nullable_double_vector clone();

  // Value equality check against another NullableScalarVec<T>
  public native @Cast("bool") boolean equals(@Const nullable_double_vector other);
}

public static class nullable_varchar_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public nullable_varchar_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public nullable_varchar_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public nullable_varchar_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public nullable_varchar_vector position(long position) {
        return (nullable_varchar_vector)super.position(position);
    }
    @Override public nullable_varchar_vector getPointer(long i) {
        return new nullable_varchar_vector((Pointer)this).offsetAddress(i);
    }

  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  public native IntPointer data(); public native nullable_varchar_vector data(IntPointer setter);              // The raw data containing all the varchars concatenated together
  public native IntPointer offsets(); public native nullable_varchar_vector offsets(IntPointer setter);           // Offsets to denote varchar start and end positions
  public native IntPointer lengths(); public native nullable_varchar_vector lengths(IntPointer setter);

  public native @Cast("uint64_t*") LongPointer validityBuffer(); public native nullable_varchar_vector validityBuffer(LongPointer setter);    // Bit vector to denote null values
  public native int dataSize(); public native nullable_varchar_vector dataSize(int setter);           // Size of data array
  public native int count(); public native nullable_varchar_vector count(int setter);              // The row count

  // Returns a deep copy of this nullable_varchar_vector
  public native nullable_varchar_vector clone();

  // Value equality check against another nullable_varchar_vector
  public native @Cast("bool") boolean equals(@Const nullable_varchar_vector other);
}

public static class non_null_double_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public non_null_double_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public non_null_double_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public non_null_double_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public non_null_double_vector position(long position) {
        return (non_null_double_vector)super.position(position);
    }
    @Override public non_null_double_vector getPointer(long i) {
        return new non_null_double_vector((Pointer)this).offsetAddress(i);
    }

    public native DoublePointer data(); public native non_null_double_vector data(DoublePointer setter);
    public native int count(); public native non_null_double_vector count(int setter);
    public int dataSize() {
        return count() * 8;
    }
}

public static class non_null_bigint_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public non_null_bigint_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public non_null_bigint_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public non_null_bigint_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public non_null_bigint_vector position(long position) {
        return (non_null_bigint_vector)super.position(position);
    }
    @Override public non_null_bigint_vector getPointer(long i) {
        return new non_null_bigint_vector((Pointer)this).offsetAddress(i);
    }

    public native @Cast("int64_t*") LongPointer data(); public native non_null_bigint_vector data(LongPointer setter);
    public native int count(); public native non_null_bigint_vector count(int setter);
    public int dataSize() {
        return count() * 8;
    }
}

public static class non_null_int2_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public non_null_int2_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public non_null_int2_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public non_null_int2_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public non_null_int2_vector position(long position) {
        return (non_null_int2_vector)super.position(position);
    }
    @Override public non_null_int2_vector getPointer(long i) {
        return new non_null_int2_vector((Pointer)this).offsetAddress(i);
    }

    public native ShortPointer data(); public native non_null_int2_vector data(ShortPointer setter);
    public native int count(); public native non_null_int2_vector count(int setter);
    public int size() {
        return count() * 2;
    }
}

public static class non_null_int_vector extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public non_null_int_vector() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public non_null_int_vector(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public non_null_int_vector(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public non_null_int_vector position(long position) {
        return (non_null_int_vector)super.position(position);
    }
    @Override public non_null_int_vector getPointer(long i) {
        return new non_null_int_vector((Pointer)this).offsetAddress(i);
    }

    public native IntPointer data(); public native non_null_int_vector data(IntPointer setter);
    public native int count(); public native non_null_int_vector count(int setter);
}

public static class non_null_c_bounded_string extends Pointer {
    static { Loader.load(); }
    /** Default native constructor. */
    public non_null_c_bounded_string() { super((Pointer)null); allocate(); }
    /** Native array allocator. Access with {@link Pointer#position(long)}. */
    public non_null_c_bounded_string(long size) { super((Pointer)null); allocateArray(size); }
    /** Pointer cast constructor. Invokes {@link Pointer#Pointer(Pointer)}. */
    public non_null_c_bounded_string(Pointer p) { super(p); }
    private native void allocate();
    private native void allocateArray(long size);
    @Override public non_null_c_bounded_string position(long position) {
        return (non_null_c_bounded_string)super.position(position);
    }
    @Override public non_null_c_bounded_string getPointer(long i) {
        return new non_null_c_bounded_string((Pointer)this).offsetAddress(i);
    }

  public native @Cast("char*") BytePointer data(); public native non_null_c_bounded_string data(BytePointer setter);
  public native int length(); public native non_null_c_bounded_string length(int setter);
}

public static final int VE_TD_DEFS = 1;
// #endif


}