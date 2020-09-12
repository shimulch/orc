package org.apache.orc.learned.data;

import org.apache.orc.learned.rmi.Prediction;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class IndexRowList extends ArrayList<IndexRow> implements Comparator<IndexRow> {
    
    public Dataset getDataset() {
        this.sortByIndexValue();
        Dataset dataset = new Dataset();

        for(int i = 0; i < this.size(); i++) {
            dataset.add(new DataRow(this.get(i).getIndexValue(), i));
        }
        return dataset;
    }

    public void sortByIndexValue() {
        this.sort(this);
    }

    public IndexRowList search(float valueToFind, Prediction prediction) {
        IndexRowList filtered = new IndexRowList();
        IndexRow searchItem = new IndexRow(valueToFind, 0, 0);
        List<IndexRow> range = this.subList(prediction.getStart(), prediction.getEnd());

        int index = Collections.binarySearch(range, searchItem, this);
        if(index > 0) {
            filtered.add(range.get(index));
        }
        return filtered;
    }

    @Override
    public int compare(IndexRow o1, IndexRow o2) {
        return Float.compare(o1.getIndexValue(), o2.getIndexValue());
    }
}
