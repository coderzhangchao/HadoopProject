package com.zhangchao.manyjob;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * atguigu--a.txt	3
 * @author fansan
 *
 */
public class TwoIndexReducer extends Reducer<Text, Text,Text, Text>{
	
	Text v = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String strv = "";
		
		for(Text value : values) {
			String s = value.toString()+" ";
			strv += s;
		}
		
		v.set(strv);
		
		context.write(key, v);
	}
}
