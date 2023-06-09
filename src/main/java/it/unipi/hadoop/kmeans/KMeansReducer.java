package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, DataPoint, Text, Text> {

    /**
     * Reducer's reduce function, performing both the cumulation of coordinates and weights of the DataPoints belonging
     * to the centroid having id number equal to the key, then calculating the mean of the coordinates
     *
     * @param key the centroid id number
     * @param values points assigned to the centroid
     * @param context the context
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(IntWritable key, Iterable<DataPoint> values, Context context) throws IOException, InterruptedException {

        DataPoint finalResult = new DataPoint(values.iterator().next());
        while (values.iterator().hasNext()) {
            finalResult.cumulatePoints(values.iterator().next());
        }

        finalResult.getCoordinates().replaceAll(aFloat -> {
            return (aFloat) / finalResult.getWeight(); //
        });

        context.write(new Text(key.toString()), new Text(finalResult.toString()));
    }
}
