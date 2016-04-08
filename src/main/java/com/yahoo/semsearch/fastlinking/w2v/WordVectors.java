package com.yahoo.semsearch.fastlinking.w2v;

/**
 * Interface defining the view of a WordVector. Classes implementing this interface must expose two methods,
 * one for getting the float[] vector given a string and another one for returning the vector length 
 * @author roi blanco
 *
 */
public interface WordVectors{    
    public float[] getVectorOf( String word );
    public int getVectorLength();
}
