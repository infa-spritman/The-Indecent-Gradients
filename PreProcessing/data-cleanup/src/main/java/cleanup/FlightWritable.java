package cleanup;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightWritable implements Writable {
	private String year;
	private String month;
	private String dayOfMonth;
	private String dayOfWeek;
	private String carrier;
	private String originAirport;
	private String destAirport;
	private String departTime;
	private String departDelay;
	private String arriveTime;
	private String arriveDelay;
	private String schecduledFlightTime;
	private String distance;

	public FlightWritable(){}

	public FlightWritable(String year,
						  String month,
						  String dayOfMonth,
						  String dayOfWeek,
						  String carrier,
						  String originAirport,
						  String destAirport,
						  String departTime,
						  String departDelay,
						  String arriveTime,
						  String arriveDelay,
						  String schecduledFlightTime,
						  String distance) {
		this.year = year;
		this.month = month;
		this.dayOfMonth = dayOfMonth;
		this.dayOfWeek = dayOfWeek;
		this.carrier = carrier;
		this.originAirport = originAirport;
		this.destAirport = destAirport;
		this.departTime = departTime;
		this.departDelay = departDelay;
		this.arriveTime = arriveTime;
		this.arriveDelay = arriveDelay;
		this.schecduledFlightTime = schecduledFlightTime;
		this.distance = distance;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeChars(year + "\n");
		dataOutput.writeChars(month + "\n");
		dataOutput.writeChars(dayOfMonth + "\n");
		dataOutput.writeChars(dayOfWeek + "\n");
		dataOutput.writeChars(carrier + "\n");
		dataOutput.writeChars(originAirport + "\n");
		dataOutput.writeChars(destAirport + "\n");
		dataOutput.writeChars(departTime + "\n");
		dataOutput.writeChars(departDelay + "\n");
		dataOutput.writeChars(arriveTime + "\n");
		dataOutput.writeChars(arriveDelay + "\n");
		dataOutput.writeChars(schecduledFlightTime + "\n");
		dataOutput.writeChars(distance + "\n");
	}

	@Override
	public void readFields(DataInput datainput) throws IOException {
		year = datainput.readLine().trim();
		month = datainput.readLine().trim();
		dayOfMonth = datainput.readLine().trim();
		dayOfWeek = datainput.readLine().trim();
		carrier = datainput.readLine().trim();
		originAirport = datainput.readLine().trim();
		destAirport = datainput.readLine().trim();
		departTime = datainput.readLine().trim();
		departDelay = datainput.readLine().trim();
		arriveTime = datainput.readLine().trim();
		arriveDelay = datainput.readLine().trim();
		schecduledFlightTime = datainput.readLine().trim();
		distance = datainput.readLine().trim();
	}

	@Override
	public String toString() {
	    return year + "," +
                month + "," +
                dayOfMonth + "," +
                dayOfWeek + "," +
                carrier + "," +
                originAirport + "," +
                destAirport + "," +
                departTime + "," +
                departDelay + "," +
                arriveTime + "," +
                arriveDelay + "," +
                schecduledFlightTime + "," +
                distance;
    }
}