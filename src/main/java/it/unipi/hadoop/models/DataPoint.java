package it.unipi.hadoop.models;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

public class DataPoint implements Writable {

    protected LinkedList<Float> coordinates;
    private int weight;


    public DataPoint(){
        coordinates = new LinkedList<>();
        weight = 1;
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

    public double squaredNorm2Distance(DataPoint p){
        double sum = 0.0;
        for (int i = 0; i < p.getCoordinates().size(); i++) {
            float difference = this.coordinates.get(i) - p.getCoordinates().get(i);
            sum += difference * difference;
        }
        System.out.println("sum:"+sum);
        return sum; //not returning the square root since we are looking for the squared norm2
    }

    public static DataPoint parseString(String s){
        DataPoint d = new DataPoint();

        for (String s2 : s.split(",")){
            d.coordinates.add(Float.parseFloat(s2));
        }
        return d;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (Float coordinate : coordinates) {
            out.writeFloat(coordinate);
        }
        out.writeInt(this.weight);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for(int i = 0; i < coordinates.size(); i++) {
            this.coordinates.set(i, in.readFloat());
        }
        this.weight = in.readInt();
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


    public DataPoint cumulatePoints(DataPoint p){
        if(this.coordinates == null){
            this.coordinates = new LinkedList<>(Collections.nCopies(p.getCoordinates().size(), 0.0f));;
        }

        for (int i=0; i<p.getCoordinates().size(); i++){
            this.coordinates.set(i, this.coordinates.get(i) + p.coordinates.get(i));
        }
        return this;
    }
}
