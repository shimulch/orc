package org.apache.orc.examples;

import org.apache.orc.learned.IndexManager;

import java.io.IOException;
import java.net.URISyntaxException;

public class LearnedOrc {

    public static void main(String[] args) throws IOException, URISyntaxException {
        IndexManager manager = new IndexManager();
        manager.index("./order.orc", 0, new int[]{10, 100, 1000});
        manager.filter("./order.orc", 1849934);
    }
}
