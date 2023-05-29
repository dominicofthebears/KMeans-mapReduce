package it.unipi.hadoop.kmeans;
import java.io.*;
import java.util.*;

import it.unipi.hadoop.models.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.server.nodemanager.Context;

public class Kmeans {

    private static Centroid[] centroids;

    //ask for the stopping condition
    private static boolean stopCondition(){
            return false;
    }

    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

    private void initializeCentroids(int k, Path inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(inputFile)));
        Random random = new Random(123);
        int numLines = countLines(String.valueOf(inputFile));
        Integer[] indexes = new Integer[k];
        indexes[0] = random.nextInt(0, numLines-k);
        int j = 0;
        int i = 0;
        String line;

        while(i<k){
            line = reader.readLine();
            if(j == indexes[i]){
                centroids[i] = (Centroid) Centroid.parseString(line);
                if(i==0){
                    indexes[i+1] = random.nextInt(indexes[0], numLines-k+i);
                }
                else{
                    indexes[i+1] = random.nextInt(indexes[i-1], numLines-k+i);
                }
                i++;
            }
            j++;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int k = Integer.parseInt(args[0]);
        Kmeans.initializeCentroids(k);

        conf.set("initializedCentroids", centroids);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        while(!stopCondition()) {
            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KMeansMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(k); //numero centroidi
            for (int i = 0; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            //FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1])); //qui salva il result del reducer?
            if(job.waitForCompletion(true)) {
                if (!stopCondition()) {

                    Path outputPath = FileOutputFormat.getOutputPath(job);
                    FileSystem fs = outputPath.getFileSystem(conf);

                    // read output file
                    FileStatus[] fileStatuses = fs.listStatus(outputPath);
                    for (FileStatus status : fileStatuses) {
                        Path filePath = status.getPath();


                        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            //gestire linea output
                            System.out.println(line);
                        }
                        reader.close();
                    }
                } else {
                    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
                }
            }
            else{
                    //job failure to handle
            }
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        System.exit(0);
    }
}

