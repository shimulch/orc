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
        List<IndexRow> range = this.subList(prediction.getStart(), Math.min(this.size(), prediction.getEnd()));

        int index = Collections.binarySearch(range, searchItem, this);

        if(index >= 0) {

            // calculate actual index on full list
            int foundAt = index + prediction.getStart();

            filtered.add(this.get(foundAt));

            int up = foundAt - 1;
            int down = foundAt + 1;

            // find all item that has same value before search result index
            while(up >= 0 && Float.compare(this.get(up).getIndexValue(), valueToFind) == 0) {
                filtered.add(this.get(up));
                up--;
            }

            // find all item that has same value after search result index
            while(down < this.size() && Float.compare(this.get(down).getIndexValue(), valueToFind) == 0) {
                filtered.add(this.get(down));
                down++;
            }

        }
        return filtered;
    }

    @Override
    public int compare(IndexRow o1, IndexRow o2) {
        return Float.compare(o1.getIndexValue(), o2.getIndexValue());
    }
}
