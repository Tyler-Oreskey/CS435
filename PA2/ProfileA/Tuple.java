package ProfileA;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tuple implements Writable {
    private Text first;
    private DoubleWritable second;

    public Tuple() {
        this.first = new Text();
        this.second = new DoubleWritable();
    }

    public Tuple(String first, double second) {
        this.first = new Text(first);
        this.second = new DoubleWritable(second);
    }

    public String getFirst() {
        return first.toString();
    }

    public double getSecond() {
        return second.get();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        first.write(arg0);
        second.write(arg0);
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        first.readFields(arg0);
        second.readFields(arg0);
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }
}
