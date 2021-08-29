package com.nec.spark.driver;

import java.util.ServiceLoader;

public interface SparkNecDriverControl {
    void init();

    void stop();

    static SparkNecDriverControl getInstance() {
        return ServiceLoader.load(SparkNecDriverControl.class).iterator().next();
    }
}
