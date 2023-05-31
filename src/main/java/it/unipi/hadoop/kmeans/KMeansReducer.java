package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, DataPoint, IntWritable, DataPoint> {


    public void reduce(IntWritable key, Iterable<DataPoint> values, Context context) throws IOException, InterruptedException {
        int totalPoints = 0;
        DataPoint finalResult = new DataPoint();

        while (values.iterator().hasNext()) {
            DataPoint c = values.iterator().next();
            totalPoints += c.getWeight();
            finalResult.cumulatePoints(c);
        }

        for (int j = 0; j < finalResult.getCoordinates().size(); j++) {
            finalResult.getCoordinates().set(j, finalResult.getCoordinates().get(j) / totalPoints);
        }

        context.write(key, finalResult);
    }
}
