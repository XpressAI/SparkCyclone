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

        public int dataSize() {
            return count * 4;
        }

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

        public int dataSize() {
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

        public int dataSize() {
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

        public int dataSize() {
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

    @Structure.FieldOrder({"data", "offsets", "validityBuffer", "dataSize", "count"})
    class nullable_varchar_vector extends Structure {
        public long data;
        public long offsets;
        public long validityBuffer;
        public Integer dataSize;
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

        public int dataSize() {
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