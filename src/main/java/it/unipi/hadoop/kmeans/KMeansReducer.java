package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, DataPoint, Text, Text> {


    public void reduce(IntWritable key, Iterable<DataPoint> values, Context context) throws IOException, InterruptedException {

        DataPoint finalResult = new DataPoint(values.iterator().next());
        int totalPoints = finalResult.getWeight();

        while (values.iterator().hasNext()) {
            DataPoint c = new DataPoint(values.iterator().next());
            totalPoints += c.getWeight();
            finalResult.cumulatePoints(c);
        }

        for (int j = 0; j < finalResult.getCoordinates().size(); j++) {
            finalResult.getCoordinates().set(j, (finalResult.getCoordinates().get(j))/totalPoints); //
        }

        context.write(new Text(key.toString()), new Text(finalResult.toString()));
    }
}
