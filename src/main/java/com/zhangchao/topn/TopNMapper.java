package com.zhangchao.topn;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
	输入数据：
	13470253144	180	180	360
	13509468723	7335	110349	117684
	13560439638	918	4938	5856
*/
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	
	// 定义一个TreeMap作为存储数据的容器（天然按key排序）
	private TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();
	private FlowBean kBean;

	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		
		kBean = new FlowBean();
		Text v = new Text();
		
		// 1 获取一行
		String line = value.toString();
		
		// 2 切割
		String[] fields = line.split("\t");
		
		// 3 封装数据
		String phoneNum = fields[0];
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		long sumFlow = Long.parseLong(fields[3]);
		
		kBean.setDownFlow(downFlow);
		kBean.setUpFlow(upFlow);
		kBean.setSumFlow(sumFlow);
		
		v.set(phoneNum);
		
		// 4 向TreeMap中添加数据
		flowMap.put(kBean, v);
		
		// 5 限制TreeMap的数据量，超过10条就删除掉流量最小的一条数据
		if (flowMap.size() > 5) {
			//flowMap.remove(flowMap.firstKey());
			flowMap.remove(flowMap.lastKey());		
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		
		// 6 遍历treeMap集合，输出数据
		Iterator<FlowBean> bean = flowMap.keySet().iterator();
		
		System.out.println("***********map阶段***********");
		while (bean.hasNext()) {
			
			FlowBean k = bean.next();
			
			System.out.println(flowMap.get(k).toString()+"============="+k.toString());
			
			context.write(k, flowMap.get(k));
		}
		System.out.println("***********map阶段***********");
	}

}
