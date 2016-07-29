package ssort.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Aphex on 29.07.2016.
 */
public class SsMapper extends Mapper<LongWritable,Text,CompositeKey,DoubleWritable>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        String symbol = tokens[0].trim();
        Integer year = Integer.parseInt(tokens[1].trim());
        Double v = Double.parseDouble(tokens[2].trim());

        CompositeKey compositeKey = new CompositeKey(symbol,year);
        DoubleWritable coinValue = new DoubleWritable(v);

        context.write(compositeKey,coinValue);
    }

}
