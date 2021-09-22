package com.nec.arrow;

import com.sun.jna.Library;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

public interface ArrowTransferStructures extends Library {

    @Structure.FieldOrder({"data", "count"})
    class non_null_int_vector extends Structure {
        public long data;
        public Integer count;

        public non_null_int_vector() {
            super();
        }

        public non_null_int_vector(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends non_null_int_vector implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }

    @Structure.FieldOrder({"data","validityBuffer", "count"})
    class nullable_int_vector extends Structure {
        public long data;
        public long validityBuffer;
        public Integer count;

        public nullable_int_vector() {
            super();
        }

        public nullable_int_vector(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends nullable_int_vector implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }

    @Structure.FieldOrder({"data", "count"})
    class non_null_int2_vector extends Structure {
        public long data;
        public Integer count;

        public non_null_int2_vector() {
            super();
        }

        public non_null_int2_vector(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends non_null_int2_vector implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }

    @Structure.FieldOrder({"data", "count"})
    class non_null_double_vector extends Structure {
        public long data;
        public Integer count;

        public int size() {
            return count * 8;
        }

        public non_null_double_vector() {
            super();
        }

        public non_null_double_vector(int count) {
            super();
            this.count = count;
        }

        public non_null_double_vector(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends non_null_double_vector implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }

    @Structure.FieldOrder({"data", "validityBuffer", "count", })
    class nullable_double_vector extends Structure {
        public long data;
        public long validityBuffer;
        public Integer count;

        public int size() {
            return count * 8;
        }

        public nullable_double_vector() {
            super();
        }

        public nullable_double_vector(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends nullable_double_vector implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }

    @Structure.FieldOrder({"data", "validityBuffer", "count", })
    class nullable_bigint_vector extends Structure {
        public long data;
        public long validityBuffer;
        public Integer count;

        public int size() {
            return count * 8;
        }

        public nullable_bigint_vector() {
            super();
        }

        public nullable_bigint_vector(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends nullable_bigint_vector implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }

    @Structure.FieldOrder({"data", "offsets", "validityBuffer", "size", "count"})
    class nullable_varchar_vector extends Structure {
        public long data;
        public long offsets;
        public long validityBuffer;
        public Integer size;
        public Integer count;
        /* 24 + 8 = 32 bytes in size */

        public nullable_varchar_vector() {
            super();
        }

        public nullable_varchar_vector(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends nullable_varchar_vector implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }

    @Structure.FieldOrder({"data", "count"})
    class non_null_bigint_vector extends Structure {
        public long data;
        public Integer count;

        public int size() {
            return count * 8;
        }

        public non_null_bigint_vector() {
            super();
        }

        public non_null_bigint_vector(int count) {
            super();
            this.count = count;
        }

        public non_null_bigint_vector(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends non_null_bigint_vector implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }

    @Structure.FieldOrder({"data", "length"})
    class non_null_c_bounded_string extends Structure {
        public long data;
        public Integer length;

        public non_null_c_bounded_string() {
            super();
        }

        public non_null_c_bounded_string(Pointer p) {
            super(p);
            read();
        }

        public static class ByReference extends non_null_c_bounded_string implements Structure.ByReference {
            public ByReference() {
            }

            public ByReference(Pointer p) {
                super(p);
            }
        }
    }
}