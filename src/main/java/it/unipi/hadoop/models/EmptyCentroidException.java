package it.unipi.hadoop.models;

public class EmptyCentroidException extends Exception{

    public EmptyCentroidException(){
        System.out.println("Empty centroid occurred, re-initializing");
    }
}
