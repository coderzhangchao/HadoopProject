package com.zhangchao.mapjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
	pd.txt
	
	01	小米
	02	华为
	03	格力
*/

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	Map<String,String> pdMap = new HashMap<String,String>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 获取缓存的文件
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		
		String line;
		while(StringUtils.isNotEmpty(line=reader.readLine())) {
			// 切割
			String[] fields = line.split("\t");
			// 缓存数据到集合
			pdMap.put(fields[0], fields[1]);
		}
		
		// 释放资源
		reader.close();
	}
	
	/*
		1001	01	1
		1002	02	2
		1003	03	3
		1004	01	4
		1005	02	5
		1006	03	6
	 */
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split("\t");
		String pId = fields[1];
		String pdName = pdMap.get(pId);
		k.set(line + "\t"+ pdName);
		context.write(k, NullWritable.get());
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
	}
}
