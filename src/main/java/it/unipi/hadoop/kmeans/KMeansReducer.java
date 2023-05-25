package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.Centroid;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

public class KMeansReducer extends Reducer<IntWritable, Centroid, IntWritable, Centroid> {


    public void reduce(IntWritable key, Iterable<Centroid> values, Context context) throws IOException, InterruptedException {
        Centroid finalResult = new Centroid();
        int totalPoints = 0;
        float totalError = 0;
        LinkedList<Float> totalPointsCoordinates = new LinkedList<>();
        float sum = 0;

        for (Centroid c : values) {
            totalError += c.getCumulatedError();
            totalPoints += c.getPointsCounter();
            for (int i = 0; i < c.getCumulatedPointsCoordinates().size(); i++) {
                sum = totalPointsCoordinates.get(i) + c.getCumulatedPointsCoordinates().get(i);
                totalPointsCoordinates.set(i, sum);
            }
        }

        finalResult.setCumulatedError(totalError);
        for (int j = 0; j < totalPointsCoordinates.size(); j++) {
            finalResult.getCumulatedPointsCoordinates().set(j, totalPointsCoordinates.get(j) / totalPoints);
        }
        context.write(key, finalResult);
    }
}
