package com.taobao.pamirs.schedule.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 目的应该是管理Zookeeper的创建，链接，链接参数之类的，有点小跑偏
 * Zookeeper的创建，初始化，检查父节点等
 */
public class ZKManager{
	
	private static transient Logger log = LoggerFactory.getLogger(ZKManager.class);
	private ZooKeeper zk;
	private List<ACL> acl = new ArrayList<ACL>();
	private Properties properties;
	private boolean isCheckParentPath = true;
	
	public enum keys {
		zkConnectString, rootPath, userName, password, zkSessionTimeout, isCheckParentPath
	}

	/**
	 * 设定properties
	 * 新建Zookeeper链接
	 * @param aProperties
	 * @throws Exception
	 */
	public ZKManager(Properties aProperties) throws Exception{
		this.properties = aProperties;
		this.connect();
	}
	
	/**
	 * 重连zookeeper
	 * @throws Exception
	 */
	public synchronized void  reConnection() throws Exception{
		if (this.zk != null) {
			this.zk.close();
			this.zk = null;
			this.connect() ;
		}
	}
	
	private void connect() throws Exception {
		CountDownLatch connectionLatch = new CountDownLatch(1);
		createZookeeper(connectionLatch);
		connectionLatch.await(10,TimeUnit.SECONDS);
	}
	
	private void createZookeeper(final CountDownLatch connectionLatch) throws Exception {
		/**
		 * 创建一个与服务器的连接
		 * 参数一：服务器地址和端口号（该端口号值服务器允许客户端连接的端口号，配置文件的默认端口号2181）
		 * 参数二：连接会话超时时间
		 * 参数三：观察者，各种事件都会触发该观察者。不过只会触发一次。
		 *      该Watcher会获取各种事件的通知
		 */
		zk = new ZooKeeper(this.properties.getProperty(keys.zkConnectString.toString()),
				Integer.parseInt(this.properties.getProperty(keys.zkSessionTimeout.toString())),
				new Watcher() {
					public void process(WatchedEvent event) {
						sessionEvent(connectionLatch, event);
					}
				});
		String authString = this.properties.getProperty(keys.userName.toString())
				+ ":"+ this.properties.getProperty(keys.password.toString());
		this.isCheckParentPath = Boolean.parseBoolean(this.properties.getProperty(keys.isCheckParentPath.toString(),"true"));
		zk.addAuthInfo("digest", authString.getBytes());
		acl.clear();
		acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
				DigestAuthenticationProvider.generateDigest(authString))));
		acl.add(new ACL(ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));
	}

//	监控被触发的事件
	private void sessionEvent(CountDownLatch connectionLatch, WatchedEvent event) {
		log.info("监控所有被触发的事件:EVENT:{}", event.getType());
		if (event.getState() == KeeperState.SyncConnected) {
			log.info("收到ZK连接成功事件！");
			connectionLatch.countDown();
		} else if (event.getState() == KeeperState.Expired ) {
			log.error("会话超时，等待重新建立ZK连接...");
			try {
				reConnection();
			} catch (Exception e) {
				log.error(e.getMessage(),e);
			}
		} // Disconnected：Zookeeper会自动处理Disconnected状态重连
		else if (event.getState() == KeeperState.Disconnected ) {
			log.info("tb_hj_schedule Disconnected，等待重新建立ZK连接...");
			try {
				reConnection();
			} catch (Exception e) {
				log.error(e.getMessage(),e);
			}
		}
		else if (event.getState() == KeeperState.NoSyncConnected ) {
			log.info("tb_hj_schedule NoSyncConnected，等待重新建立ZK连接...");
			try {
				reConnection();
			} catch (Exception e) {
				log.error(e.getMessage(),e);
			}
		}
		else{
			log.info("tb_hj_schedule 会话有其他状态的值，event.getState() ="+event.getState() +", event  value="+event.toString());
			connectionLatch.countDown();
		}
	}
	
	public void close() throws InterruptedException {
		log.info("关闭zookeeper连接");
		if(zk == null) {
 		    return;
		}
		this.zk.close();
	}

	/**
	 * 默认的ZK配置信息
	 * @return
	 */
	public static Properties createProperties(){
		Properties result = new Properties();
		result.setProperty(keys.zkConnectString.toString(),"localhost:2181");
		result.setProperty(keys.rootPath.toString(),"/taobao-pamirs-schedule/huijin");
		result.setProperty(keys.userName.toString(),"ScheduleAdmin");
		result.setProperty(keys.password.toString(),"password");
		result.setProperty(keys.zkSessionTimeout.toString(),"60000");
		result.setProperty(keys.isCheckParentPath.toString(),"true");
		
		return result;
	}
	public String getRootPath(){
		return this.properties.getProperty(keys.rootPath.toString());
	}
	public String getConnectStr(){
		return this.properties.getProperty(keys.zkConnectString.toString());
	}
	public boolean checkZookeeperState() throws Exception{
		return zk != null && zk.getState() == States.CONNECTED;
	}

	/**
	 * 初始化zk的配置根路径，检查并设置版本号
	 * @throws Exception
	 */
	public void initial() throws Exception {
		//当zk状态正常后才会调用以下的数据
		if(zk.exists(this.getRootPath(), false) == null){
//			创建root节点
			ZKTools.createPath(zk, this.getRootPath(), CreateMode.PERSISTENT, acl);
			if(isCheckParentPath == true){
//				判断所有父目录是不是做过之前版本的控制节点
			  checkParent(zk,this.getRootPath());
			}
			//设置控制节点版本号 tbschedule-3.2.12
			zk.setData(this.getRootPath(),Version.getVersion().getBytes(),-1);
		}else{
			//先校验父亲节点，本身是否已经是taobao-pamirs-schedule版本的控制节点，是则抛异常
			if(isCheckParentPath == true){
			   checkParent(zk,this.getRootPath());
			}
			byte[] value = zk.getData(this.getRootPath(), false, null);
			if(value == null){
				zk.setData(this.getRootPath(),Version.getVersion().getBytes(),-1);
			}else{
				String dataVersion = new String(value);
//				查看是否兼容之前的版本
				if(Version.isCompatible(dataVersion)==false){
					throw new Exception("TBSchedule程序版本 "+ Version.getVersion() +" 不兼容Zookeeper中的数据版本 " + dataVersion );
				}
				log.info("当前的程序版本:" + Version.getVersion() + " 数据版本: " + dataVersion);
			}
		}
	}
	public static void checkParent(ZooKeeper zk, String path) throws Exception {
		String[] list = path.split("/");
		String zkPath = "";
		for (int i =0;i< list.length -1;i++){
			String str = list[i];
			if ( !str.equals("")) {
				zkPath = zkPath + "/" + str;
				if (zk.exists(zkPath, false) != null) {
					/**
					 * Zookeeper 监视（Watches） 简介
					 * Zookeeper 中最有特色且最不容易理解的是监视(Watches)。
					 * Zookeeper 所有的读操作
					 * 	getData()
					 *  getChildren()
					 *  exists() 都 可以设置监视(watch).
					 *  监视事件可以理解为一次性的触发器， 官方定义如下： a watch event is one-time trigger,
					 *  sent to the client that set the watch, which occurs when the data for which the watch was set changes。
					 *  对此需要作出如下理解：
					 *  （一次性触发）One-time trigger
					 *  	当设置监视的数据发生改变时，该监视事件会被发送到客户端，
					 *  	例如，如果客户端调用了 getData("/znode1", true)
					 *  	并且稍后 /znode1 节点上的数据发生了改变或者被删除了，客户端将会获取到 /znode1 发生变化的监视事件，
					 *  	而如果 /znode1 再一次发生了变化，除非客户端再次对 /znode1 设置监视，否则客户端不会收到事件通知。
					 *
					 *  （发送至客户端）Sent to the client
					 *  	Zookeeper 客户端和服务端是通过 socket 进行通信的，
					 *  	由于网络存在故障，所以监视事件很有可能不会成功地到达客户端，监视事件是异步发送至监视者的，
					 *  	Zookeeper 本身提供了保序性(ordering guarantee)：
					 *  		即客户端只有首先看到了监视事件后，才会感知到它所设置监视的 znode 发生了变化
					 *  		(a client will never see a change for which it has set a watch until it first sees the watch event).
					 *  		网络延迟或者其他因素可能导致不同的客户端在不同的时刻感知某一监视事件，但是不同的客户端所看到的一切具有一致的顺序。
					 *
					 *  （设置 watch 的数据）The data for which the watch was set
					 *  	这意味着 znode 节点本身具有不同的改变方式。
					 *  	你也可以想象 Zookeeper 维护了两条监视链表：
					 *  	数据监视和子节点监视(data watches and child watches)
					 *  		getData() and exists() 设置数据监视，getChildren() 设置子节点监视。
					 *  	或者，你也可以想象 Zookeeper 设置的不同监视返回不同的数据，
					 *  		getData() 和 exists() 返回 znode 节点的相关信息，
					 *  		而 getChildren() 返回子节点列表。
					 *  	因此， setData() 会触发设置在某一节点上所设置的数据监视(假定数据设置成功)，
					 *  	而一次成功的 create() 操作则会出发当前节点上所设置的数据监视以及父节点的子节点监视。
					 *  	一次成功的 delete() 操作将会触发当前节点的数据监视和子节点监视事件，同时也会触发该节点父节点的child watch。
					 *
					 *  Zookeeper 中的监视是轻量级的，因此容易设置、维护和分发。
					 *  当客户端与 Zookeeper 服务器端失去联系时，客户端并不会收到监视事件的通知，
					 *  只有当客户端重新连接后，若在必要的情况下，以前注册的监视会重新被注册并触发，对于开发人员来说 这通常是透明的。
					 *  只有一种情况会导致监视事件的丢失，即：通过 exists() 设置了某个 znode 节点的监视，
					 *  但是如果某个客户端在此 znode 节点被创建和删除的时间间隔内与 zookeeper 服务器失去了联系，
					 *  该客户端即使稍后重新连接 zookeeper服务器后也得不到事件通知。
					 */
//					获取节点下的数据，不做监视
//						检查是否之前使用过这个节点，作为TBSchedule的控制节点
// 					当前TBSchedule的版本号是tbschedule-3.2.12
//					排除之前的版本号，可能是不兼容的
//					只要之前使用了 taobao-pamirs-schedule- ，就报异常
//					取得zkPath节点下的数据,返回byte[]
					byte[] value = zk.getData(zkPath, false, null);
					if(value != null){
						String tmpVersion = new String(value);
						log.info("节点版本：{}", tmpVersion);
					   if(tmpVersion.indexOf("taobao-pamirs-schedule-") >=0){
						throw new Exception("\"" + zkPath +"\"  is already a schedule instance's root directory, its any subdirectory cannot as the root directory of others");
					}
				}
			}
			}
		}
	}	
	
	public List<ACL> getAcl() {
		return acl;
	}
	public ZooKeeper getZooKeeper() throws Exception {
		if(this.checkZookeeperState()==false){
			reConnection();
		}
		return this.zk;
	}
	
}
