package com.hadoop.mapreduce.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {
    public static final int INPUT_SIZE = 1000;
    public static final int CLUSTERS = 5;

    /**
     * @param args
     */
    public static void main(String[] args) {
        // Generate a list of items.
        List<DoublePoint> inputs = new ArrayList<DoublePoint>();
        Random generator = new Random();

        for (int i = 0; i < 100; i++) {
            inputs.add(new DoublePoint(generator.nextDouble(), generator.nextDouble()));
        }
        System.out.println("inputs: "+inputs);

        KMeans clusterer = new KMeans();
        System.out.println("Clustering items");
        List<DoublePoint> results = clusterer.cluster(inputs, CLUSTERS);
        DoublePoint point;

        for (int i = 0; i < results.size(); i++) {
            point = results.get(i);
            System.out.println("Cluster " + (i + 1) + ": " + point.toString());
        }
    }

}
