package count;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightCarrierKey implements WritableComparable<FlightCarrierKey>
{
    private int distance;
    private int carrier;
    private int time;

    public FlightCarrierKey(){}

    public FlightCarrierKey(int distance, int carrier, int time) {
        this.distance = distance;
        this.carrier = carrier;
        this.time = time;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public int getCarrier() {
        return carrier;
    }

    public void setCarrier(int carrier) {
        this.carrier = carrier;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(distance);
        out.writeInt(carrier);
        out.writeInt(time);
    }

    public void readFields(DataInput in) throws IOException {
        distance = in.readInt();
        carrier = in.readInt();
        time = in.readInt();
    }

    public int compareTo(FlightCarrierKey key) {
        int comp;
        if ((comp = Integer.compare(distance, key.distance)) != 0)
            return comp;
        else if ((comp = Integer.compare(carrier, key.carrier)) != 0)
            return comp;
        else return Integer.compare(time, key.time);
    }

    public int hashCode() {
        String h = "" + distance;
        h += carrier;
        h += time;
        return h.hashCode();
    }

    public String toString() {
        return "(" + distance + "," + carrier + "," + time + ")";
    }
}
