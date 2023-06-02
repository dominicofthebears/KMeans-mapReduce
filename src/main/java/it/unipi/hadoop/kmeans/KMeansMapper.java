package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, DataPoint>
{

    private DataPoint[] centroids;


    public void setup(Context context) throws IOException, InterruptedException {
        int k = Integer.parseInt(context.getConfiguration().get("k"));
        Configuration conf = context.getConfiguration();

        this.centroids = new DataPoint[k];

        for(int j=0;j<k;j++){
            String s = conf.get("centroid"+j);
            centroids[j] = new DataPoint(s);
        }
    }


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            float minDistance = Float.POSITIVE_INFINITY;
            int closestLabel = 0;
            float distance;

            DataPoint dataPoint = new DataPoint(value.toString());
            for (int i=0; i<centroids.length; i++){
                distance = dataPoint.squaredNorm2Distance(centroids[i]);
                if(distance<minDistance){
                    minDistance = distance;
                    closestLabel = i;
                }
            }
            context.write(new IntWritable(closestLabel), dataPoint);
        }
    }

