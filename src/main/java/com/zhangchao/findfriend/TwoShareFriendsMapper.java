package com.zhangchao.findfriend;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
	A	I,K,C,B,G,F,H,O,D,
	B	A,F,J,E,
	C	A,E,B,H,F,G,K,
	D	G,C,K,A,L,F,E,H,
	
	A-B	E C 
	A-C	D F 
	A-D	E F 
	A-E	D B C 
	A-F	O B C D E 
	A-G	F E C D 
	A-H	E C D O 
*/
public class TwoShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// A I,K,C,B,G,F,H,O,D,
		// 友 人，人，人
		String line = value.toString();
		String[] friend_persons = line.split("\t");

		String friend = friend_persons[0];
		String[] persons = friend_persons[1].split(",");

		Arrays.sort(persons);

		for (int i = 0; i < persons.length - 1; i++) {
			
			for (int j = i + 1; j < persons.length; j++) {
				// 发出 <人-人，好友> ，这样，相同的“人-人”对的所有好友就会到同1个reduce中去
				context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
			}
		}
	}
}
