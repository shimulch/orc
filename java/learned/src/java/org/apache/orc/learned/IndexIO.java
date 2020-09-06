package org.apache.orc.learned;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;
import org.apache.orc.learned.data.IndexRow;
import org.apache.orc.learned.data.IndexRowList;
import org.apache.orc.learned.rmi.Predictor;
import org.apache.orc.learned.rmi.Trainer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class IndexIO {

    Configuration conf;

    private static final String INDEX_ORC_SCHEMA = "struct<valueToIndex:double,stripeId:int,stripeRowId:int>";
    public IndexIO(Configuration conf) throws URISyntaxException {
        this.conf = conf;
    }

    public void write(Trainer trainer, IndexRowList indexRows, String filename) throws IOException {
        Path file = new Path(filename);
        TypeDescription schema = TypeDescription.fromString(INDEX_ORC_SCHEMA);
        Writer writer = OrcFile.createWriter(file, OrcFile.writerOptions(conf).setSchema(schema).overwrite(true));
        VectorizedRowBatch batch = schema.createRowBatch();
        DoubleColumnVector valueToIndex = (DoubleColumnVector) batch.cols[0];
        LongColumnVector stripeId = (LongColumnVector) batch.cols[1];
        LongColumnVector stripeRowId = (LongColumnVector) batch.cols[2];

        int rowCounter;
        for (IndexRow row: indexRows) {
            rowCounter = batch.size++;
            valueToIndex.vector[rowCounter] = row.getIndexValue();
            stripeId.vector[rowCounter] = row.getStripeId();
            stripeRowId.vector[rowCounter] = row.getStripeRowId();
            if(batch.getMaxSize() == batch.size) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        if(batch.size > 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        System.out.println("Serialized Data: " + trainer.serialize());
        writer.addUserMetadata("RMI", ByteBuffer.wrap(trainer.serialize().getBytes(StandardCharsets.UTF_8)));
        writer.close();
    }


    public Predictor read(String filename) throws IOException {
        Path file = new Path(filename);
        Reader reader = OrcFile.createReader(file, OrcFile.readerOptions(conf));
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        RecordReader recordReader = reader.rows();

        IndexRowList indexRows = new IndexRowList();

        DoubleColumnVector valueToIndex = (DoubleColumnVector) batch.cols[0];
        LongColumnVector stripeId = (LongColumnVector) batch.cols[1];
        LongColumnVector stripeRowId = (LongColumnVector) batch.cols[2];

        while (recordReader.nextBatch(batch)) {
            for(int row = 0; row < batch.size; row++) {
                indexRows.add(new IndexRow((float) valueToIndex.vector[row], (int) stripeId.vector[row], (int) stripeRowId.vector[row]));
            }
        }

        ByteBuffer byteBuffer = reader.getMetadataValue("RMI");
        byte[] userMetaByte = new byte[byteBuffer.remaining()];
        byteBuffer.get(userMetaByte);
        String indexData = new String(userMetaByte, StandardCharsets.UTF_8);
        Predictor predictor = Predictor.parse(indexData);
        predictor.setIndexRowList(indexRows);
        return predictor;
    }
}
