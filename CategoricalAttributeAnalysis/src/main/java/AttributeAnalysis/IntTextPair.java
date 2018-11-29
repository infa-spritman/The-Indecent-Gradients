package AttributeAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntTextPair implements WritableComparable<IntTextPair> {

    private IntWritable first = new IntWritable();
    private Text second = new Text();

    public void set(int left, String right) {
        first.set(left);
        second.set(right);
    }

    public IntWritable getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public int hashCode() {
        return first.hashCode() + second.hashCode();
    }


    @Override
    public boolean equals(Object right) {
        if (right instanceof IntTextPair) {
            IntTextPair r = (IntTextPair) right;
            return r.first.get() == first.get() && r.second.equals(second);
        } else {
            return false;
        }
    }

    public String toString(){
        return this.first.get() + "," + this.second.toString();
    }

    @Override
    public int compareTo(IntTextPair o) {
        if (first.get() != o.first.get()) {
            return first.get() < o.first.get() ? -1 : 1;
        } else {
            return second.compareTo(o.getSecond());
        }
    }

}
