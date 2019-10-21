package com.zhangchao.outformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 输入：
	http://www.baidu.com
	http://www.google.com
	http://cn.bing.com
	http://www.atguigu.com
	http://www.sohu.com
	http://www.sina.com
	http://www.sin2a.com
	http://www.sin2desa.com
	http://www.sindsafa.com
*/

/*
	atguigu.log
	http://www.atguigu.com
	
	other.log
	http://www.baidu.com
	http://www.google.com
	http://cn.bing.com
	http://www.sohu.com
	http://www.sina.com
	http://www.sin2a.com
	http://www.sin2desa.com
	http://www.sindsafa.com
*/
	
public class OutFromatMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.write(value, NullWritable.get());
	}
}
