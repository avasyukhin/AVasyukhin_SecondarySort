package ssort.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Aphex on 29.07.2016.
 */
public class NaturalKeyGroupingComparator extends WritableComparator{
    protected NaturalKeyGroupingComparator(){
        super(CompositeKey.class,true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;

        return -1*key1.getSymbol().compareTo(key2.getSymbol());
    }
}
