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

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Centroid>
{

    private Centroid[] centroids;

    //after each ended iteration we write the new centroids on the conf file and then retrieve them here
    //should we use the conf file or a .txt file to save them? We can write each time on the output file the centroids
    //which format should the output have
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
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                DataPoint dataPoint = DataPoint.parseString(itr.nextToken(),false);

                for (int i=0; i<centroids.length; i++){
                    if(dataPoint.squaredNorm2Distance(centroids[i])<minDistance){
                        minDistance = (float) dataPoint.squaredNorm2Distance(centroids[i]);
                        closestLabel = i;
                    }
                }
                centroids[closestLabel].setCumulatedError(centroids[closestLabel].getCumulatedError() + minDistance);
                centroids[closestLabel].setPointsCounter(centroids[closestLabel].getPointsCounter() + 1);

                //inMapper combiner
                for(int j=0; j<dataPoint.getCoordinates().size(); j++){
                    float val = centroids[closestLabel].getCumulatedPointsCoordinates().get(j);
                    centroids[closestLabel].getCumulatedPointsCoordinates().set(j, val+dataPoint.getCoordinates().get(j));
                }


            }
            context.write(new IntWritable(closestLabel), centroids[closestLabel]);
        }
    }

