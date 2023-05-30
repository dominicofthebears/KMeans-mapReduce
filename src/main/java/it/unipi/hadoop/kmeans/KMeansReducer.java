package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.Centroid;
import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, Centroid, IntWritable, DataPoint> {


    public void reduce(IntWritable key, Iterable<Centroid> values, Context context) throws IOException, InterruptedException {
        int totalPoints = 0;
        float totalError = 0;
        DataPoint finalResult = new DataPoint();
        float sum = 0;

        while (values.iterator().hasNext()) {
            Centroid c = values.iterator().next();
            totalError += c.getCumulatedError();
            totalPoints += c.getPointsCounter();
            finalResult.cumulatePoints(c);
        }
        //finalResult.setCumulatedError(totalError);
        for (int j = 0; j < finalResult.getCoordinates().size(); j++) {
            finalResult.getCoordinates().set(j, finalResult.getCoordinates().get(j) / totalPoints);
        }
        context.write(key, finalResult);
    }
}
