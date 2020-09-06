package org.apache.orc.learned.rmi;

public class ModelError {
    private int minErr, maxErr;

    public ModelError(int minErr, int maxErr) {
        this.minErr = minErr;
        this.maxErr = maxErr;
    }

    public int getMinErr() {
        return minErr;
    }

    public void setMinErr(int minErr) {
        this.minErr = minErr;
    }

    public int getMaxErr() {
        return maxErr;
    }

    public void setMaxErr(int maxErr) {
        this.maxErr = maxErr;
    }
}
