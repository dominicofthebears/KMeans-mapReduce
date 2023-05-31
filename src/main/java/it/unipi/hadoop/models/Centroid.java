package it.unipi.hadoop.models;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Centroid extends DataPoint implements Writable {

    private int pointsCounter;

    public Centroid(){
        this.pointsCounter = 0;
        this.coordinates = new LinkedList<>();
    }

    public int getPointsCounter() {
        return pointsCounter;
    }

    public void setPointsCounter(int pointsCounter) {
        this.pointsCounter = pointsCounter;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(this.pointsCounter);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.pointsCounter = in.readInt();
    }

    @Override
    public String toString(){
        StringBuilder centroid = new StringBuilder();
        for (int i = 0; i < coordinates.size(); i++) {
            centroid.append(this.coordinates.get(i));
            if(i != coordinates.size() - 1) {
                centroid.append(",");
            }
        }
        return centroid.toString();
    }

    public static Centroid parseString(String s){
        Centroid c = new Centroid();
        for (String s2 : s.split(",")){
            c.coordinates.add(Float.parseFloat(s2));
        }
        return c;
    }

    public Centroid cumulatePoints(DataPoint p){
        if(this.coordinates == null){
            this.coordinates = new LinkedList<>(Collections.nCopies(p.getCoordinates().size(), 0.0f));;
        }

        for (int i=0; i<p.getCoordinates().size(); i++){
            this.coordinates.set(i, this.coordinates.get(i) + p.coordinates.get(i));
        }
        return this;
    }

}
