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


    /**
     * Checks if the algorithm has reached the stop condition
     *
     * @param oldCentroids centroids of the previous iteration
     * @param newCentroids centroid of this iteration
     * @param nIterations number of iteration the algorithm has done
     * @param maxIterations maximum number of iteration before the end of the algorithm
     * @param threshold value of distance we want to reach in order to end the algorithm
     *
     * @return true: the algorithm is over, false: the algorithm continues with another iteration
     */
    private static boolean stopCondition(DataPoint[] oldCentroids, DataPoint[] newCentroids, int nIterations, int maxIterations, float threshold){
        float totDistance=0;

        for(int i=0; i<newCentroids.length; i++){ //cumulates the distances between old and new centroids
            totDistance += newCentroids[i].squaredNorm2Distance(oldCentroids[i]);
        }

        if(totDistance<=threshold || nIterations>=maxIterations) { //if the total distance is lower than the threshold the algorithm can end
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Counts the number of lines of the input file
     *
     * @param filename the name of input file
     *
     * @return the number of lines, or 1 if the file is empty
     *
     * @throws IOException
     */
    private static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(Files.newInputStream(Paths.get(filename))); //opening of an inputstream
        try {
            byte[] c = new byte[1024]; //allocated byte to read from the file
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false; //the file is not empty
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') { //end of line
                        ++count; //increments the number of read lines
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close(); //closure of the inputstream
        }
    }

    /**
     * Initialize centroids for first Job iteration
     *
     * @param k is the number of centroids
     * @param inputFile is the file containing the starting dataset
     */
    private static void initializeCentroids(int k, Path inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(inputFile)));
        Random random = new Random();
        int numLines = countLines(String.valueOf(inputFile));

        Integer[] indexes = new Integer[k]; //vector that contain the lines to read from the dataset file
        indexes[0] = random.nextInt(numLines-k);
        int j = 0;
        int i = 0;
        String line;

        centroids= new DataPoint[k]; //initialize centroid vector

        while(i < k){
            line = reader.readLine(); //read each line of the dataset
            if(j == indexes[i]){
                centroids[i] = new DataPoint(line); //the i-th centroids take the value on that line
                if(i < k-1) {
                    //the next value of the vector will contain a value on the interval between the value of the precedent
                    //element of the vector and the last one possible - k + i
                    indexes[i + 1] = random.nextInt((numLines - k + i) - indexes[i] + 1) + indexes[i] + 1;
                }
                i++;
            }
            j++;
        }
    }

    /**
     * Read the centroids from the latest iteration, with the objective to compare these centroids with the older ones
     *
     * @param conf  the configuration variable
     * @param outputFile the output file of the latest iteration, in which the job prints the centroids
     * @param k the centroids number
     *
     * @return the set of centroids read on the outputFile
     */
    private static DataPoint[] readCentroids(Configuration conf, String outputFile, int k) throws IOException {
        Path outputPath = new Path(outputFile);
        FileSystem fs = FileSystem.get(conf);

        DataPoint[] newCentroids=new DataPoint[k];

        // read centroids from job's output file
        FileStatus[] fileStatuses = fs.listStatus(outputPath);
        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();

            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));

            if(!status.getPath().toString().endsWith("_SUCCESS")) {
                String line= reader.readLine();
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
            conf.set("centroid" + c, centr.toString());
            c++;
        }
        //delete job's file output after reading it
        fs.delete(outputPath, true);
        return newCentroids;
    }

    /**
     * Write the set of centroids of the last job (after reaching the stop condition) on a file
     *
     * @param conf configuration variable
     * @param outputFile file where write the last centroids
     */
    private static void writeFinalOutput(Configuration conf, String outputFile) throws IOException {
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
        int c=0;
        boolean stop = false;
        int i=0;

        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml"));

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int k = Integer.parseInt(otherArgs[0]);
        conf.set("k", String.valueOf(k));

        Path path_dataset= new Path("../"+otherArgs[1]);
        Kmeans.initializeCentroids(k,path_dataset);

        int maxIteration = Integer.parseInt(otherArgs[2]);
        float threshold = Float.parseFloat(otherArgs[3]);


        for(DataPoint centroid: centroids){
                conf.set("centroid"+c,centroid.toString());
                c++;
        }


        while(!stop) {
            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setNumReduceTasks(k);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DataPoint.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            if(job.waitForCompletion(true)) {
                DataPoint[] newCentroids = readCentroids(conf, otherArgs[otherArgs.length - 1], k);
                stop = stopCondition(centroids,newCentroids,i,maxIteration, threshold);
                if (!stop) {
                        for(int j=0; j<newCentroids.length; j++){
                            centroids[j]=new DataPoint(newCentroids[j]);}
                }
                else {
                    writeFinalOutput(conf,otherArgs[otherArgs.length - 1]);
                }
            }

            else{
                    //job is failed
                    System.err.println("Job "+i+"-th failed");
                    System.exit(1);  //exit after error occured
            }
            i++;
        }

        System.exit(0);
    }
}

