package org.apache.orc.learned.data;

public class IndexRow {

    private float indexValue;

    private int stripeId;

    private int stripeRowId;

    public IndexRow(float indexValue, int stripeId, int stripeRowId) {
        this.indexValue = indexValue;
        this.stripeId = stripeId;
        this.stripeRowId = stripeRowId;
    }

    public float getIndexValue() {
        return indexValue;
    }

    public void setIndexValue(float indexValue) {
        this.indexValue = indexValue;
    }

    public int getStripeId() {
        return stripeId;
    }

    public void setStripeId(int stripeId) {
        this.stripeId = stripeId;
    }

    public int getStripeRowId() {
        return stripeRowId;
    }

    public void setStripeRowId(int stripeRowId) {
        this.stripeRowId = stripeRowId;
    }

    @Override
    public String toString() {
        return "IndexRow{" +
                "indexValue=" + indexValue +
                ", stripeId=" + stripeId +
                ", stripeRowId=" + stripeRowId +
                '}';
    }
}
