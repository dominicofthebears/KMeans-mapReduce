package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.Centroid;
import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, Centroid, IntWritable, Centroid> {

    public void reduce(IntWritable key, Iterable<Centroid> values, Context context) throws IOException, InterruptedException {
        Centroid cumulator = new Centroid();
        int numPoints = 0;

        while (values.iterator().hasNext()) {
            cumulator.cumulatePoints(values.iterator().next());
            numPoints += 1;
        }

        cumulator.setPointsCounter(numPoints);
        context.write(key, cumulator);
    }
}
