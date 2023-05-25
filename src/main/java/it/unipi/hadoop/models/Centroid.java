package it.unipi.hadoop.models;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Centroid extends DataPoint implements Writable {

    private int pointsCounter;
    private float cumulatedError;
    private int label;
    private LinkedList<Float> cumulatedPointsCoordinates;

    public void Centroid(){
        this.pointsCounter = 0;
        this.cumulatedError = 0;
        this.coordinates = new LinkedList<>();
        this.cumulatedPointsCoordinates = new LinkedList<>();
    }


    public int getPointsCounter() {
        return pointsCounter;
    }

    public void setPointsCounter(int pointsCounter) {
        this.pointsCounter = pointsCounter;
    }

    public float getCumulatedError() {
        return cumulatedError;
    }

    public void setCumulatedError(float cumulatedError) {
        this.cumulatedError = cumulatedError;
    }

    public int getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.pointsCounter);
        out.writeInt(this.label);
        out.writeFloat(this.cumulatedError);
        for(int i = 0; i < coordinates.size(); i++) {
            out.writeFloat(this.coordinates.get(i));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.pointsCounter = in.readInt();
        this.label = in.readInt();
        this.cumulatedError = in.readFloat();
        for(int i = 0; i < coordinates.size(); i++) {
            this.coordinates.set(i, in.readFloat());
        }
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

    public List<Float> getCumulatedPointsCoordinates() {
        return cumulatedPointsCoordinates;
    }

    public void setCumulatedPointsCoordinates(List<Float> cumulatedPointsCoordinates) {
        this.cumulatedPointsCoordinates = (LinkedList<Float>) cumulatedPointsCoordinates;
    }
}
