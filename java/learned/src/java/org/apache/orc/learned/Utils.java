package org.apache.orc.learned;

public class Utils {
    public static Integer predictionToInteger(Float prediction) {
        int pred = Math.round(prediction);
        return pred > -1 ? pred : 0;

    }
}
