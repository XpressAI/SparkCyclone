package com.nec.spark.driver;

import java.util.ServiceLoader;

public interface SparkNecDriverControl {
    void init(Object obj);

    static SparkNecDriverControl getInstance() {
        return ServiceLoader.load(SparkNecDriverControl.class).iterator().next();
    }
}
