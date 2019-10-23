package com.zhangchao.manyjob;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*

	输出：
	
	atguigu	c.txt-->2	b.txt-->2	a.txt-->3	
	pingping	c.txt-->1	b.txt-->3	a.txt-->1	
	ss	c.txt-->1	b.txt-->1	a.txt-->2
	
	第一次输出
	atguigu--a.txt 3
	atguigu--b.txt 2
	atguigu--c.txt 2

*/
public class OneIndexReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	LongWritable v = new LongWritable();
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		
		long sum = 0;
		
		for(LongWritable value : values) {
			sum += value.get();
		}
		
		v.set(sum);
		
		context.write(key, v);
	}

}
