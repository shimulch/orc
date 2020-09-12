package org.apache.orc.proof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class TPCDataConverter {

    public void writeFromRefData(String refDataDirName, String schemaFileName, String outputFileName) throws IOException, ParseException {
        File refDataDir = new File(refDataDirName);
        File files[] = refDataDir.listFiles();
        File schemaFile = new File(schemaFileName);
        JSONObject jsonObject = (JSONObject) new JSONParser().parse(new FileReader(schemaFile));
        String seperator = (String) jsonObject.getOrDefault("separator", "|");
        HashMap<String, JSONObject> loadSchema = (HashMap<String, JSONObject>) jsonObject.get("dataSchema");
        JSONArray export = (JSONArray) jsonObject.get("export");

        ArrayList<String> schemaSegments = new ArrayList<>();
        SortedMap<Integer, String> collectorMap = new TreeMap<>();
        loadSchema.forEach((k, v) -> {
            String name = (String) v.get("name");
            String type = (String) v.get("type");
            if(export.stream().filter(i -> i.equals(name)).count() > 0) {
                schemaSegments.add(name + ":" + type);
                collectorMap.put(Integer.parseInt(k), type);
            }
        });

        Configuration conf = new Configuration();
        String orcSchema = "struct<" + String.join(",", schemaSegments) + ">";

        TypeDescription schema = TypeDescription.fromString(orcSchema);
        Writer writer = OrcFile.createWriter(new Path(outputFileName), OrcFile.writerOptions(conf).setSchema(schema));
        VectorizedRowBatch batch = schema.createRowBatch();
        SortedMap<Integer, ColumnVector> columnVectorMap = new TreeMap<>();
        int counter = 0;
        for(Map.Entry<Integer, String> entry: collectorMap.entrySet()) {
            columnVectorMap.put(entry.getKey(), batch.cols[counter++]);
        }

        for(File file: files) {
            if (!file.getName().equals(schemaFile.getName())) {
                Scanner scanner = new Scanner(file);
                while (scanner.hasNextLine()) {
                    String[] line = scanner.nextLine().split(Pattern.quote(seperator));
                    int row = batch.size++;
                    for(Map.Entry<Integer, String> entry: collectorMap.entrySet()) {
                        String value = line[entry.getKey()];
                        switch (entry.getValue()) {
                            case "double":
                                ((DoubleColumnVector) columnVectorMap.get(entry.getKey())).vector[row] = Double.parseDouble(value);
                                break;
                            default:
                                ((LongColumnVector) columnVectorMap.get(entry.getKey())).vector[row] = Long.parseLong(value);
                                break;
                        }
                    }

                    if(batch.size == batch.getMaxSize()) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }
                scanner.close();
            }
        }

        if(batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
        }

        writer.close();
    }
}
