package com.nec.arrow; 

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(
    value = @Platform(
	compiler = "cpp17",
        include = {
//            "words.hpp",
//            "char_int_conv.hpp",
//            "parsefloat.hpp",
            "transfer-definitions.hpp"
        }
//	link = "jniTransferDefinitions"
    ),
    target = "com.nec.arrow.TransferDefinitions"
)
public class TransferDefinitionsConfig implements InfoMapper {
    public void map(InfoMap infoMap) {
        infoMap.put(new Info("std::vector<std::string>").pointerTypes("StringVector").define())
               .put(new Info("std::vector<size_t>").pointerTypes("SizeTVector").define());
    }
}
