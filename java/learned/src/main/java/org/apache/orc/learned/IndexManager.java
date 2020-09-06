package org.apache.orc.learned;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.learned.data.*;
import org.apache.orc.learned.rmi.Prediction;
import org.apache.orc.learned.rmi.Predictor;
import org.apache.orc.learned.rmi.Trainer;

import java.io.IOException;
import java.net.URISyntaxException;

public class IndexManager {

    public void index(String orcFilePath, int columnIndex) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(orcFilePath), OrcFile.readerOptions(conf));
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        RecordReader recordReader = reader.rows(reader.options());
        ColumnVector indexColumn =  batch.cols[columnIndex];

        IndexRowList rowList = new IndexRowList();

        int stripeRowId = 0;
        int currentStripe = recordReader.getCurrentStripe();

        while (recordReader.nextBatch(batch)) {
            if(recordReader.getCurrentStripe() != currentStripe) {
                currentStripe = recordReader.getCurrentStripe();
                stripeRowId = 0;
            }
            for(int r = 0; r < batch.size; r++) {
                double valueToIndex = 0.0;

                if(indexColumn.type == ColumnVector.Type.LONG) {
                    valueToIndex = ((LongColumnVector) indexColumn).vector[r];
                } else {
                    valueToIndex = ((DoubleColumnVector) indexColumn).vector[r];
                }
                rowList.add(new IndexRow((float) valueToIndex, currentStripe, stripeRowId));
                stripeRowId++;
            }
        }

        Trainer trainer = new Trainer(rowList.getDataset(), new int[]{10, 100, 1000});
        trainer.train();
        trainer.calculateErrors();
        IndexIO indexIO = new IndexIO(conf);
        indexIO.write(trainer, rowList, this.getIndexFileName(orcFilePath));
    }

    private String getIndexFileName(String orcFilePath) {
        return orcFilePath + ".index";
    }

    public void filter(String orcFilePath, float valueToFind) throws URISyntaxException, IOException {
        IndexIO indexIO = new IndexIO(new Configuration());
        Predictor predictor = indexIO.read(this.getIndexFileName(orcFilePath));
        Prediction prediction = predictor.predict(valueToFind);
        IndexRowList result = predictor.getIndexRowList().search(valueToFind, prediction);

        System.out.println("Prediction: " + predictor.predict(valueToFind));
        System.out.println("Result: " + result);
    }
}
