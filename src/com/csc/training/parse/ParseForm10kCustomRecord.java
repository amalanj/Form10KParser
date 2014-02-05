package com.csc.training.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ParseForm10kCustomRecord implements WritableComparable<ParseForm10kCustomRecord>{
	
	public ParseForm10kCustomRecord(){}
	
	public ParseForm10kCustomRecord(String companyName, String units, String actualContent) {
		this.companyName = companyName;
		this.units = units;
		this.actualContent = actualContent;
	}

	private String companyName;
	
	private String actualContent;
	
	private String units;
		
	public String getUnits() {
		return units;
	}

	public void setUnits(String units) {
		this.units = units;
	}

	public String getCompanyName() {
		return companyName;
	}

	public String getActualContent() {
		return actualContent;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public void setActualContent(String actualContent) {
		this.actualContent = actualContent;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.companyName = in.readUTF();
		this.units = in.readUTF();
		this.actualContent = in.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(companyName);
		out.writeUTF(units);
		out.writeUTF(this.actualContent);
		
	}

	@Override
	public int compareTo(ParseForm10kCustomRecord other) {
		
		if (this.companyName.compareTo(other.companyName) != 0) {
			return this.companyName.compareTo(other.companyName);
		} else if (this.actualContent.compareTo(other.actualContent) != 0) {
			return this.actualContent.compareTo(other.actualContent);
		} else {
			return 0;
		}
		
	}

}
