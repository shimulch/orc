package org.apache.orc.learned.rmi;

import org.apache.orc.learned.Utils;
import org.apache.orc.learned.data.IndexRowList;
import org.apache.orc.learned.models.LinearModel;
import org.apache.orc.learned.models.Model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

public class Predictor {


    private IndexRowList indexRowList;

    private int[] stages;

    private ArrayList<ArrayList<Model>> modelIndex;
    private ArrayList<ModelError> leafNodeErrors;

    public Predictor(int[] stages, ArrayList<ArrayList<Model>> modelIndex, ArrayList<ModelError> leafNodeErrors) {
        this.stages = stages;
        this.modelIndex = modelIndex;
        this.leafNodeErrors = leafNodeErrors;
    }

    public IndexRowList getIndexRowList() {
        return indexRowList;
    }

    public void setIndexRowList(IndexRowList indexRowList) {
        this.indexRowList = indexRowList;
    }

    public static Predictor parse(String serialized) {
        String[] separateStageModel = serialized.split("~");
        String stageString = separateStageModel[0];
        String modelString = separateStageModel[1];
        String errorString = separateStageModel[2];
        int[] stages = Arrays.stream(stageString.split(",")).mapToInt(Integer::parseInt).toArray();

        String[] modelParams = modelString.split(Pattern.quote("|"));
        ArrayList<ArrayList<Model>> modelIndex = new ArrayList<>();
        int modelNumber = 0;
        for (int s = 0; s < stages.length; s++) {
            modelIndex.add(new ArrayList<>());
            for (int m = 0; m < stages[s]; m++) {
                Model model = new LinearModel();
                model.load(modelParams[modelNumber]);
                modelIndex.get(s).add(model);
            }
        }
        ArrayList<ModelError> errors = new ArrayList<>();
        Arrays.stream(errorString.split(Pattern.quote("|"))).forEach(i -> {
            String[] modelErrorString = i.split(",");
            errors.add(new ModelError(Integer.parseInt(modelErrorString[0]), Integer.parseInt(modelErrorString[1])));
        });

        return new Predictor(stages, modelIndex, errors);
    }

    public Prediction predict(float x) {
        int i = 0;
        int j = 0;
        int prediction = 0;
        while (i < stages.length) {
            prediction = Utils.predictionToInteger(modelIndex.get(i).get(j).predict(x));
            if (i + 1 < stages.length) {
                j = prediction % stages[i + 1];
            }
            i++;
        }
        ModelError err = leafNodeErrors.get(j);
        return new Prediction(
                Math.max(0, prediction - err.getMinErr()),
                prediction + err.getMaxErr(),
                prediction
        );
    }

}
