package org.apache.orc.learned.models;


import org.apache.orc.learned.data.DataRow;
import org.apache.orc.learned.data.Dataset;

public class LinearModel implements Model {

    LinearModelParams params;

    public LinearModelParams getParams() {
        return params;
    }

    public void setParams(LinearModelParams params) {
        this.params = params;
    }

    public void learn(Dataset dataset) {
        float mean_x = (float) 0.0;
        float mean_y = (float) 0.0;
        float c = (float) 0.0;
        int n = 0;
        float m2 = (float) 0.0;

        int data_size = 0;
        for(DataRow row: dataset) {
            n += 1;
            float dx = row.getX() - mean_x;
            mean_x += dx / n;
            mean_y += (row.getY() - mean_y) / n;
            c += dx * (row.getY() - mean_y);

            float dx2 = row.getX() - mean_x;
            m2 += dx * dx2;
            data_size += 1;
        }

        // special case when we have 0 or 1 items
        if (data_size == 0) {
            this.params = new LinearModelParams(0.0f, 0.0f);

        } else if (data_size == 1) {
            this.params = new LinearModelParams(mean_y, 0.0f);
        } else {

            float cov = c / (n - 1);
            Float var = m2 / (n - 1);
            assert !(var >= 0.0);

            if (var.equals(0.0)) {
                // variance is zero. pick the mean (only) value.
                this.params = new LinearModelParams(mean_y, 0.0f);
            } else {

                float beta = cov / var;
                float alpha = mean_y - beta * mean_x;

                this.params = new LinearModelParams(alpha, beta);
            }
        }
    }

    public float predict(float inp) {
        return this.params.alpha + this.params.beta * inp;
    }

    @Override
    public String name() {
        return "linear";
    }

    @Override
    public String serialize() {
        return this.params.serialize();
    }

    @Override
    public void load(String serialized) {
        this.setParams(LinearModelParams.parse(serialized));
    }

    public static class LinearModelParams {
        float alpha, beta;

        public LinearModelParams(float alpha, float beta) {
            this.alpha = alpha;
            this.beta = beta;
        }

        public String serialize() {
            return alpha + "," + beta;
        }

        public static LinearModelParams parse(String paramStr) {
            String[] paramSplitted = paramStr.trim().split(",");
            return new LinearModelParams(
                    Float.parseFloat(paramSplitted[0]),
                    Float.parseFloat(paramSplitted[1])
            );
        }

        @Override
        public String toString() {
            return "LinearModelParams{" +
                    "alpha=" + alpha +
                    ", beta=" + beta +
                    '}';
        }
    }
}
