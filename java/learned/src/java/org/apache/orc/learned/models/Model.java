package org.apache.orc.learned.models;

import org.apache.orc.learned.data.Dataset;

public interface Model {
    public String name();
    public String serialize();
    public void load(String serialized);
    public void learn(Dataset dataset);
    public float predict(float inp);
}
