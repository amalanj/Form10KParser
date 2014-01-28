package com.csc.training.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ParseForm10kMapKey implements WritableComparable<ParseForm10kMapKey>{
	
	public ParseForm10kMapKey(){}
	
	public ParseForm10kMapKey(String companyName, int year) {
		this.companyName = companyName;
		this.year = year;
	}

	private String companyName;
	
	private int year;
		
	public String getCompanyName() {
		return companyName;
	}

	public int getYear() {
		return year;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public void setYear(int year) {
		this.year = year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.companyName = in.readUTF();
		this.year = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(companyName);
		out.writeInt(this.year);
		
	}

	@Override
	public int compareTo(ParseForm10kMapKey other) {
		
		if (this.companyName.compareTo(other.companyName) != 0) {
			return this.companyName.compareTo(other.companyName);
		} else if (this.year != other.year) {
			return year < other.year ? -1 : 1;
		} else {
			return 0;
		}
		
	}

}
