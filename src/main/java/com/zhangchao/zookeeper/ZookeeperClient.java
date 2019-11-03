package com.zhangchao.zookeeper;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperClient {
	
	private static int sessionTimeout = 2000;
	private String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	private ZooKeeper zkClient = null;
	
	@Before
	public void init() throws Exception {
		
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				// 收到事件通知后的回调函数（用户的业务逻辑）
				System.out.println(event.getType() + "--" + event.getPath());

				// 再次启动监听
				try {
					zkClient.getChildren("/", true);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		});
	}
	
	//创建节点
	@Test
	public void create() throws Exception{
		
		// 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
		String nodeCreated = zkClient.create("/atguigu", "jinlian".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		System.out.println("nodeCreated====" + nodeCreated);
	}
	
	//获取字节点
	@Test
	public void getChildren() throws Exception{
		
		List<String> childrens = zkClient.getChildren("/sanguo", true);
		
		for(String children : childrens) {
			System.out.println(children);
		}
		
		// 延时阻塞
		Thread.sleep(Long.MAX_VALUE);
	}
	
	
	//判断节点是否存在
	@Test
	public void exist() throws Exception{
		
		Stat stat = zkClient.exists("/atguigu", false);
		
		System.out.println( null == stat ? "不存在" : "存在");
	}
}
