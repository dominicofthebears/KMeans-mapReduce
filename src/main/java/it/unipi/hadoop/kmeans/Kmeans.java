package it.unipi.hadoop.kmeans;
import java.io.*;
import java.util.*;

import it.unipi.hadoop.models.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmeans {

    private static Centroid[] centroids;

    //ask for the stopping condition
    private static boolean stopCondition(Centroid[] oldCentroids, Centroid[] newCentroids, int nIterations, int maxIterations, float threshold){
        float totDistance=0;
        for(int i=0; i<newCentroids.length; i++){
            totDistance += newCentroids[i].squaredNorm2Distance(oldCentroids[i]);
        }
        if(totDistance<=threshold) //if the total distance is lower than the threshold the algorithm can end
            return true;
        else if(nIterations>=maxIterations) // if the distance is higher than the threshold the algorithm stops anyway if it runs for at least maxIterations iterations
            return true;
        else
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

        centroids= new Centroid[k]; //initialize centroid vector

        //System.out.println("indice:"+indexes[0]);

        while(i<k && j<1000){
            line = reader.readLine();
            //System.out.println("line"+j+": "+line);
            //System.out.println("indice"+i+": "+indexes[i]);
            if(j == indexes[i]){
                centroids[i] = (Centroid) Centroid.parseString(line,true);  //DOMENICO look
                if(i==0){
                    //indexes[i+1] = random.nextInt(indexes[0], numLines-k+i);
                    indexes[i+1] = random.nextInt((numLines - k + i) - indexes[0] + 1) + indexes[0]+1;  //il +1 nella parentesi è per prendere il secondo estremo compreso, e il +1 qui fuori per prendere il primo estremo NON compreso, altrimenti può succedere di estrarre un valore uguale a quello estratto al passaggio precedente

                }
                else{
                    //indexes[i+1] = random.nextInt(indexes[i-1], numLines-k+i);
                    if(i<k-1) { //se k=20, i=19 abbiamo assegnato tutti e 20 i posti dell-array indexes, qui entriamo e prende indexes[20] che è OutOfBounds
                        indexes[i + 1] = random.nextInt((numLines - k + i) - indexes[i] + 1) + indexes[i] + 1;
                    }
                }
                i++;
            }
            j++;
        }
    }

    private static Centroid[] readCentroids(Configuration conf, String outputFile, int k) throws IOException {
        Path outputPath = new Path(outputFile);
        FileSystem fs = outputPath.getFileSystem(conf);

        Centroid[] newCentroids=new Centroid[k];

        // read output file
        FileStatus[] fileStatuses = fs.listStatus(outputPath);
        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();


            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
            String line;
            while ((line = reader.readLine()) != null) {
                //gestire linea output
                //each line should be a centroid
                String[] splitOfLine=line.split("\t");
                int centroidId=Integer.parseInt(splitOfLine[0]);
                String[] coordinates=splitOfLine[1].split(",");
                LinkedList<Float> coordinatesCentroid=new LinkedList<>();
                for(String coordinate :coordinates){
                    coordinatesCentroid.add(Float.parseFloat(coordinate));
                }
                Centroid centroid=new Centroid();
                centroid.setCoordinates(coordinatesCentroid);
                newCentroids[centroidId]=centroid;
            }
            conf.set("initializedCentroids", Arrays.toString(newCentroids));
            reader.close();
            if (fs.exists(outputPath)) { //in this way it will avoid the Output directory already exists problem, deleting each time the outputPath before rewrite to it
                fs.delete(outputPath, true);
            }
        }
        return newCentroids;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int k = Integer.parseInt(otherArgs[0]);
        conf.set("k", String.valueOf(k));

        Path path_dataset= new Path("../"+otherArgs[1]);
        Kmeans.initializeCentroids(k,path_dataset);

        int c=0;
        for(Centroid centroid: centroids){
                conf.set("centroid"+c,centroid.toString());
                c++;
        }
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        int stop=0;
        int i=0;

        while(stop==0) {

            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KMeansMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(k); //numero centroidi
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1])); //qui salva il result del reducer?
            if(job.waitForCompletion(true)) {


                Centroid[] newCentroids = readCentroids(conf, otherArgs[otherArgs.length - 1], k);
                if (!stopCondition(centroids,newCentroids,i,10, 30)) {
                        stop=0;
                } else {
                    stop=1;
                    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
                }
            }
            else{
                    //job failure to handle
            }
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
            i++;
        }
        System.exit(0);
    }
}

