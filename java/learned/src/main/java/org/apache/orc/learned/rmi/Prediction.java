package org.apache.orc.learned.rmi;

public class Prediction {

    private int start, end, prediction;

    public Prediction(int start, int end, int prediction) {
        this.start = start;
        this.end = end;
        this.prediction = prediction;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public int getPrediction() {
        return prediction;
    }

    public void setPrediction(int prediction) {
        this.prediction = prediction;
    }

    @Override
    public String toString() {
        return "Prediction{" +
                "start=" + start +
                ", end=" + end +
                ", prediction=" + prediction +
                '}';
    }
}
