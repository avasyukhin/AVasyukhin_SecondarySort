package ssort.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Aphex on 29.07.2016.
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey,DoubleWritable>{
    @Override
    public int getPartition(CompositeKey key, DoubleWritable val, int numPartitions){
        int hash = key.getSymbol().hashCode();
        int parition = hash % numPartitions;
        return parition;
    }
}
