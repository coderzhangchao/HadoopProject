package com.zhangchao.outformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	
	Text k = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> value,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		
		// 1 获取一行
		String line = key.toString();

		// 2 拼接
		line = line + "\r\n";

		// 3 设置key
		k.set(line);

		// 4 输出
		context.write(k, NullWritable.get());
	}
}
