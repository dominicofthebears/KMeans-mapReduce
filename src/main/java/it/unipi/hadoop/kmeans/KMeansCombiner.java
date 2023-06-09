package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, DataPoint, IntWritable, DataPoint> {

    /**
     * Combiner's reduce function, performing a first cumulation of coordinates and weights of the DataPoints
     *
     * @param key the nearest' centroid id number
     * @param values subset of points assigned to that specific centroid
     * @param context the context
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(IntWritable key, Iterable<DataPoint> values, Context context) throws IOException, InterruptedException {
        DataPoint cumulator = new DataPoint(values.iterator().next());
        while (values.iterator().hasNext()) {
            cumulator.cumulatePoints(values.iterator().next());
        }

        context.write(key, cumulator);
    }
}
