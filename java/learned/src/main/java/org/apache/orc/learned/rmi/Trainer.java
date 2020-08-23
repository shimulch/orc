package org.apache.orc.learned.rmi;

import org.apache.orc.learned.Utils;
import org.apache.orc.learned.data.DataRow;
import org.apache.orc.learned.data.Dataset;
import org.apache.orc.learned.models.LinearModel;
import org.apache.orc.learned.models.Model;

import java.util.ArrayList;

public class Trainer {
    private Dataset dataset;

    private int[] stages;

    private ArrayList<ArrayList<Model>> modelIndex;
    private ArrayList<ModelError> leafNodeErrors;

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public Trainer(Dataset dataset, int[] stages) {
        this.dataset = dataset;
        this.stages = stages;
    }

    public void train() {

        ArrayList<ArrayList<Dataset>> tmpRecords = new ArrayList<>(stages.length);
        modelIndex = new ArrayList<>(stages.length);
        for (int i = 0; i < stages.length; i++) {
            modelIndex.add(i, new ArrayList<>(stages[i]));
            ArrayList<Dataset> datasets = new ArrayList<>();
            for (int j = 0; j < stages[i]; j++) {

                if (i == 0 && j == 0) {
                    datasets.add(j, this.dataset);
                } else {
                    datasets.add(j, new Dataset());
                }
            }
            tmpRecords.add(i, datasets);
        }


        // for each stage
        for (int i = 0; i < stages.length; i++) {

            // for each model in stage
            for (int j = 0; j < stages[i]; j++) {
                // train the model
                Model model = new LinearModel();
                model.learn(tmpRecords.get(i).get(j));
                modelIndex.get(i).add(j, model);

                if (i + 1 < stages.length) {
                    for (DataRow d : tmpRecords.get(i).get(j)) {
                        int prediction = Utils.predictionToInteger(model.predict(d.getX())) % stages[i + 1];
                        tmpRecords.get(i + 1).get(prediction).add(d);
                    }
                }

            }
        }

        // calculate leaf node errors
        leafNodeErrors = new ArrayList<>();
        int leaf = stages.length - 1;
        for (int i = 0; i < stages[leaf]; i++) {
            int minErr = 0, maxErr = 0;
            for (DataRow row : tmpRecords.get(leaf).get(i)) {
                int actual = Utils.predictionToInteger(row.getY());
                int prediction = Utils.predictionToInteger(modelIndex.get(leaf).get(i).predict(row.getX()));
                int err = actual - prediction;
                if (err < 0) {
                    minErr = Math.max(minErr, Math.abs(err));
                } else {
                    maxErr = Math.max(maxErr, err);
                }
            }
            leafNodeErrors.add(new ModelError(minErr, maxErr));
        }
    }

    public void calculateErrors() {

    }

    /**
     * Serialized RMI to be stored as string in Index ORC
     * Format: stages~params~errors
     *
     * @return serializedString
     * @example: 10, 100, 200~3.4,2.2|2.1,2.3|0.1,2~20,32|12,3
     */
    public String serialize() {

        ArrayList<String> serialized = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        for (int s = 0; s < stages.length; s++) {
            builder.append(stages[s]);
            if (s + 1 < stages.length) {
                builder.append(",");
            }
            for (int m = 0; m < stages[s]; m++) {
                serialized.add(modelIndex.get(s).get(m).serialize());
            }
        }

        builder.append("~").append(String.join("|", serialized));

        builder.append("~");
        for (int i = 0; i < leafNodeErrors.size(); i++) {
            if (i > 0) {
                builder.append('|');
            }
            builder.append(leafNodeErrors.get(i).getMinErr());
            builder.append(',');
            builder.append(leafNodeErrors.get(i).getMaxErr());
        }

        return builder.toString();
    }

    public Predictor getPrediction() {
        return new Predictor(this.stages, this.modelIndex, this.leafNodeErrors);
    }
}
