package ssort.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Aphex on 29.07.2016.
 */
public class SsReducer extends Reducer<CompositeKey,DoubleWritable,Text,Text>{
    @Override
    public void reduce(CompositeKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Text k = new Text(key.getSymbol());

        Iterator<DoubleWritable> iterator =values.iterator();
        while (iterator.hasNext()){
            Text v = new Text(iterator.next().toString());
            context.write(k,v);
        }
    }
}
