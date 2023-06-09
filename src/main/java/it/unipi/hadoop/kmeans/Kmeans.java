package it.unipi.hadoop.kmeans;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import it.unipi.hadoop.models.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmeans {

    private static DataPoint[] centroids;


    private static boolean stopCondition(DataPoint[] oldCentroids, DataPoint[] newCentroids, int nIterations, int maxIterations, float threshold){
        System.out.println("start stopCondition function");
        float totDistance=0;

        for(int i=0; i<newCentroids.length; i++){
            totDistance += newCentroids[i].squaredNorm2Distance(oldCentroids[i]);
        }

        if(totDistance<=threshold || nIterations>=maxIterations) { //if the total distance is lower than the threshold the algorithm can end
            System.out.println("end stopCondition function");
            return true;
        }
        else {
            System.out.println("end stopCondition function");
            return false;
        }
    }

    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(Files.newInputStream(Paths.get(filename)));
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

    private static void initializeCentroids(int k, Path inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(inputFile)));
        Random random = new Random();
        int numLines = countLines(String.valueOf(inputFile));
        //System.out.println("num_righeFile:"+numLines);
        Integer[] indexes = new Integer[k];
        indexes[0] = random.nextInt(numLines-k);
        int j = 0;
        int i = 0;
        String line;

        centroids= new DataPoint[k]; //initialize centroid vector

        while(i<k){
            line = reader.readLine();
            if(j == indexes[i]){
                centroids[i] = new DataPoint(line);
                if(i<k-1) { //se k=20, i=19 abbiamo assegnato tutti e 20 i posti dell-array indexes, qui entriamo e prende indexes[20] che è OutOfBounds
                    indexes[i + 1] = random.nextInt((numLines - k + i) - indexes[i] + 1) + indexes[i] + 1;
                }
                i++;
            }
            j++;
        }
    }

    private static DataPoint[] readCentroids(Configuration conf, String outputFile, int k) throws IOException {
        Path outputPath = new Path(outputFile);
        FileSystem fs = FileSystem.get(conf);

        DataPoint[] newCentroids=new DataPoint[k];

        // read output file
        FileStatus[] fileStatuses = fs.listStatus(outputPath);
        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();


            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));

            //int i=0;
            //while ((line = reader.readLine()) != null) {
                //gestire linea output
                //each line should be a centroid
            System.out.println("pathOutputfile:"+status.getPath().toString());
            if(!status.getPath().toString().endsWith("_SUCCESS")) {
                String line= reader.readLine();
                System.out.println("line:" + line);
                String[] splitOfLine = line.split("\t");
                int centroidId = Integer.parseInt(splitOfLine[0]);
                String coordinates = splitOfLine[1];
                newCentroids[centroidId] = new DataPoint(coordinates);

                reader.close();
            }
        }
        int c = 0;
        //we have to update the centroids in the conf file for the next job
        for (DataPoint centr : newCentroids) {
            System.out.println("centr:"+centr.toString());
            conf.set("centroid" + c, centr.toString());
            c++;
        }
        fs.delete(outputPath, true); //in this way it will avoid the Output directory already exists problem, deleting each time the outputPath before rewrite to it
        return newCentroids;
    }

    public static void writeFinalOutput(Configuration conf, String outputFile) throws IOException {
        Path outputPath = new Path(outputFile);
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fs_output=fs.create(outputPath,true);

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs_output));

        for(int i=0; i<Integer.parseInt(conf.get("k"));i++){
                writer.write(conf.get("centroid"+i));
                writer.newLine();
        }
        writer.close();
        fs.close();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml"));


        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int k = Integer.parseInt(otherArgs[0]);
        conf.set("k", String.valueOf(k));

        Path path_dataset= new Path("../"+otherArgs[1]);
        Kmeans.initializeCentroids(k,path_dataset);

        int maxIteration = Integer.parseInt(otherArgs[2]);
        float threshold = Float.parseFloat(otherArgs[3]);

        int c=0;
        for(DataPoint centroid: centroids){
                conf.set("centroid"+c,centroid.toString());
                c++;
        }
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        for(int i=0;i<k;i++){
            System.out.println("centroid"+i+":"+conf.get("centroid"+i));
        }

        boolean stop = false;
        int i=0;

        while(!stop) {
            System.out.println("cycle number:"+i);

            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            //job.setMapOutputKeyClass(IntWritable.class);
            //job.setMapOutputValueClass(DataPoint.class);
            job.setNumReduceTasks(k); //numero centroidi
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DataPoint.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1])); //qui salva il result del reducer?
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            if(job.waitForCompletion(true)) {

                System.out.println("Entro nella condizione di stop dopo aver effettuato la prima iterazione");

                DataPoint[] newCentroids = readCentroids(conf, otherArgs[otherArgs.length - 1], k);
                stop = stopCondition(centroids,newCentroids,i,maxIteration, threshold);
                if (!stop) { //leggere maxIterations e threshold da parametri
                        //we have to insert in centroids (containing our oldCentroids) the newCentroids
                        //that for the next cycle will be the old one
                        for(int j=0; j<newCentroids.length; j++){
                            centroids[j]=new DataPoint(newCentroids[j]); //using the copy constructor
                        }
                } else {
                    writeFinalOutput(conf,otherArgs[otherArgs.length - 1]);
                }
            }
            else{
                    //job failure to handle
                    System.err.println("Job "+i+"-th failed");
                    System.exit(1);  //exit after error occured
            }
            //System.exit(job.waitForCompletion(true) ? 0 : 1);

            i++;
        }
        System.exit(0);
    }
}

