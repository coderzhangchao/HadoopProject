package com.zhangchao.outformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

public class FilterOutputFormat extends FileOutputFormat<Text,NullWritable>{

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		return new FilterRecordWriter(job);
	}
}
