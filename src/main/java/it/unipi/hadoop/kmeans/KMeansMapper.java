package it.unipi.hadoop.kmeans;

import it.unipi.hadoop.models.Centroid;
import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.server.nodemanager.Context;

import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, DataPoint>
{

    private Centroid[] centroids;

    //modify the setup to retrieve the centroids from file
    public void setup(Context context) throws IOException, InterruptedException {
        int k = Integer.parseInt(context.getConfiguration().get("k"));
        centroids = new Centroid[k];
        Configuration conf = context.getConfiguration();
        //String parsedCentroids = conf.get("initializedCentroids");
        //System.out.println("centroids:"+parsedCentroids);
        int i = 0;
        //for (String s : parsedCentroids.split("\n")){
        for(int j=0;j<k;j++){
            String s = conf.get("centroid"+j);
            centroids[i] = (Centroid) Centroid.parseString(s,true);
            centroids[i].setLabel(i);
            i++;
        }
    }

    //ask if the map processes one point per time
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            float minDistance = Float.POSITIVE_INFINITY;
            int closestLabel = 0;

            DataPoint dataPoint = DataPoint.parseString(String.valueOf(value));


            for (int i=0; i<centroids.length; i++){
                if(dataPoint.squaredNorm2Distance(centroids[i])<minDistance){
                    minDistance = (float) dataPoint.squaredNorm2Distance(centroids[i]);
                    closestLabel = i;
                }
            }
            context.write(new IntWritable(closestLabel), dataPoint);
        }
    }

