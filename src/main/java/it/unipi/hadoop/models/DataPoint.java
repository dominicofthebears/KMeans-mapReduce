package it.unipi.hadoop.models;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

public class DataPoint implements Writable {

    protected LinkedList<Float> coordinates;
    private int weight;
    private int numDimensions;


    public DataPoint(DataPoint d){  //copy constructor

        this.coordinates = new LinkedList<>();
        this.numDimensions = d.getNumDimensions();
        this.coordinates.addAll(d.coordinates);
        weight = d.getWeight();
        numDimensions = d.getNumDimensions();
    }

    public DataPoint(String s) {
        this.coordinates = new LinkedList<>();
        weight = 1;
        numDimensions = 0;
        for (String s2 : s.split(",")){
            this.coordinates.add(Float.parseFloat(s2));
            numDimensions += 1;
        }
    }


    public LinkedList<Float> getCoordinates() {
        return coordinates;
    }
    public void setCoordinates(LinkedList<Float> coordinates) {
        this.coordinates = coordinates;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public float squaredNorm2Distance(DataPoint p){
        float sum = 0.0f;
        for (int i = 0; i < p.getCoordinates().size(); i++) {
            float difference = this.coordinates.get(i) - p.getCoordinates().get(i);
            sum += difference * difference;
        }
        return sum;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.weight);
        out.writeInt(this.numDimensions);
        for (Float coordinate : coordinates) {
            out.writeFloat(coordinate);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.weight = in.readInt();
        this.numDimensions = in.readInt();
        coordinates = new LinkedList<>();
        for(int i = 0; i < numDimensions; i++) {
            coordinates.add(in.readFloat());
        }

    }

    public String toString(){
        StringBuilder dataPoint = new StringBuilder();
        for (int i = 0; i < coordinates.size(); i++) {
            dataPoint.append(this.coordinates.get(i));
            if(i != coordinates.size() - 1) {
                dataPoint.append(",");
            }
        }
        return dataPoint.toString();
    }


    public void cumulatePoints(DataPoint p){
        for (int i=0; i<p.getCoordinates().size(); i++){
            this.coordinates.set(i, this.coordinates.get(i) + p.coordinates.get(i));
        }
        this.weight += p.getWeight();
    }

    public int getNumDimensions() {
        return numDimensions;
    }

    public void setNumDimensions(int numDimensions) {
        this.numDimensions = numDimensions;
    }
}
