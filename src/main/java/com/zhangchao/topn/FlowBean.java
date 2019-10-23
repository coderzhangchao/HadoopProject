package com.zhangchao.topn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
	输入数据：
	13470253144	180	180	360
	13509468723	7335	110349	117684
	13560439638	918	4938	5856
*/
public class FlowBean implements WritableComparable<FlowBean>{
	
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	public FlowBean() {
		super();
	}
	
	
	
	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow ;
	}

	public void set(long upFlow,long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public int compareTo(FlowBean bean) {
		
		int result;
		
		if (this.sumFlow > bean.getSumFlow()) {
			result = -1;
		}else if (this.sumFlow < bean.getSumFlow()) {
			result = 1;
		}else {
			result = 0;
		}
		
		return result;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

}
