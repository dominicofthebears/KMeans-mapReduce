package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, DataPoint, IntWritable, DataPoint> {

    public void reduce(IntWritable key, Iterable<DataPoint> values, Context context) throws IOException, InterruptedException {
        DataPoint cumulator = new DataPoint();
        int numPoints = 0;

        while (values.iterator().hasNext()) {
            cumulator.cumulatePoints(values.iterator().next());
            numPoints += 1;
        }

        cumulator.setWeight(numPoints);
        context.write(key, cumulator);
    }
}
