package com.zhangchao.manyjob;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
	atguigu--a.txt	3
	atguigu--b.txt	2
	atguigu--c.txt	2
	pingping--a.txt	1
	pingping--b.txt	3
	pingping--c.txt	1
	ss--a.txt	2
	ss--b.txt	1
	ss--c.txt	1
	
	输出：
	
	atguigu	c.txt-->2	b.txt-->2	a.txt-->3	
	pingping	c.txt-->1	b.txt-->3	a.txt-->1	
	ss	c.txt-->1	b.txt-->1	a.txt-->2

*/
public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	Text k = new Text();
	Text v = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split("--");
		k.set(fields[0]);
		v.set(fields[1].replaceAll("\t", "-->"));
		context.write(k, v);
	}
}
