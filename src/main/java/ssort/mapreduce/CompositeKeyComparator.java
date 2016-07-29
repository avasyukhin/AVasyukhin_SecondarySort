package ssort.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Aphex on 29.07.2016.
 */
public class CompositeKeyComparator extends WritableComparator{

    protected CompositeKeyComparator(){
        super(CompositeKey.class,true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;

        int result = -1*key1.getSymbol().compareTo(key2.getSymbol());
        if (0==result){
            result = key1.getYear().compareTo(key2.getYear());
        }

        return result;
    }
}
