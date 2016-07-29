package ssort.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Created by Aphex on 29.07.2016.
 */
public class CompositeKey implements WritableComparable<CompositeKey>{

    private String symbol;
    private Integer year;

    public CompositeKey() {
    }

    public CompositeKey(String symbol, Integer year) {
        this.symbol = symbol;
        this.year = year;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    @Override
    public String toString() {
        return (new StringBuilder())
                .append('{')
                .append(symbol)
                .append(',')
                .append(year)
                .append('}')
                .toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        symbol = WritableUtils.readString(in);
        year = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, symbol);
        out.writeInt(year);
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = symbol.compareTo(o.symbol);
        if(0 == result) {
            result = year.compareTo(o.year);
        }
        return result;
    }
}
