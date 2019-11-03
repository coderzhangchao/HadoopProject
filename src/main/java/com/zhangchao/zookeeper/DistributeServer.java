package com.zhangchao.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 监控服务器节点上下线
 * @author fansan
 *
 */
public class DistributeServer {

	private static int sessionTimeout = 2000;
	private String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	private ZooKeeper zkClient = null;
	private String parentNode = "/servers";
	
	public void getConn() throws Exception{
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
	}
	
	//注册服务器
	public void registServer(String hostName) throws Exception{
		
		String create = zkClient.create(parentNode+"/server", hostName.getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(hostName +" is online "+ create);
	}
	
	// 业务功能
	public void business(String hostname) throws Exception{
		System.out.println(hostname+" is working ...");
		
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void main(String[] args) throws Exception{
		// 1获取zk连接
		DistributeServer server = new DistributeServer();
		
		server.getConn();
				
		// 2 利用zk连接注册服务器信息
		server.registServer(args[0]);
				
		// 3 启动业务功能
		server.business(args[0]);

	}

}
