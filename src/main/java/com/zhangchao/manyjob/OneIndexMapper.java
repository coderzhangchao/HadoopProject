package com.zhangchao.manyjob;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
	a.txt
	
	atguigu pingping
	atguigu ss
	atguigu ss
	
	b.txt
	
	atguigu pingping
	atguigu pingping
	pingping ss
	
	c.txt
	atguigu ss
	atguigu pingping
	
	输出：
	
	atguigu	c.txt-->2	b.txt-->2	a.txt-->3	
	pingping	c.txt-->1	b.txt-->3	a.txt-->1	
	ss	c.txt-->1	b.txt-->1	a.txt-->2
	
	第一次输出
	atguigu--a.txt 3
	atguigu--b.txt 2
	atguigu--c.txt 2
*/
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
	String name;
	Text k = new Text();
	LongWritable v = new LongWritable(1l);
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		name = fileSplit.getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split(" ");
		for(String field : fields) {
			k.set(field+"--"+name);
			context.write(k, v);
		}
	}
	
}
