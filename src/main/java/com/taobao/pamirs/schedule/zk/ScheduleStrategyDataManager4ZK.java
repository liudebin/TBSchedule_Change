package com.taobao.pamirs.schedule.zk;

import java.io.Writer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.taobao.pamirs.schedule.strategy.ManagerFactoryInfo;
import com.taobao.pamirs.schedule.strategy.ScheduleStrategy;
import com.taobao.pamirs.schedule.strategy.ScheduleStrategyRuntime;
import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 调度策略的数据管理类
 */
public class ScheduleStrategyDataManager4ZK{
	
	private ZKManager zkManager;
	private String PATH_Strategy; // /{root}/strategy 持久性路径
	private String PATH_ManagerFactory; // /{root}/factory 持久性路径
	private Gson gson ;
	private Logger logger = LoggerFactory.getLogger(getClass());

	//调度策略信息路径 = /{root}/strategy 持久性路径
	//创建调度器的工厂路径 = /{root}/factory 持久性路径
    public ScheduleStrategyDataManager4ZK(ZKManager aZkManager) throws Exception {
    	this.zkManager = aZkManager;
		gson = new GsonBuilder().registerTypeAdapter(Timestamp.class,new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();		
		this.PATH_Strategy = this.zkManager.getRootPath() +  "/strategy";
		this.PATH_ManagerFactory = this.zkManager.getRootPath() +  "/factory";
		
		if (this.getZooKeeper().exists(this.PATH_Strategy, false) == null) {
			ZKTools.createPath(getZooKeeper(),this.PATH_Strategy, CreateMode.PERSISTENT, this.zkManager.getAcl());
		}
		if (this.getZooKeeper().exists(this.PATH_ManagerFactory, false) == null) {
			ZKTools.createPath(getZooKeeper(),this.PATH_ManagerFactory, CreateMode.PERSISTENT, this.zkManager.getAcl());
		}
	}

	/**
	 * 返回调度策略对象
     * 调度策略节点存储的值就是策略对象，反序列化成ScheduleStrategy对象，返回
	 *
	 * 根据策略名称，获取策略信息
	 * @param strategyName
	 * @return
	 * @throws Exception
	 */
	public ScheduleStrategy loadStrategy(String strategyName)
			throws Exception {
		String zkPath = this.PATH_Strategy + "/" + strategyName;
		if(this.getZooKeeper().exists(zkPath, false) == null){
			return null;
		}
		String valueString= new String(this.getZooKeeper().getData(zkPath,false,null));
//		{"strategyName":"testStrategy","IPList":["127.0.0.1"],"numOfSingleServer":3,"assignNum":4,"kind":"Schedule","taskName":"myTbScheduleJob1","taskParameter":"1-2,2-3","sts":"resume"}
		ScheduleStrategy result = (ScheduleStrategy)this.gson.fromJson(valueString, ScheduleStrategy.class);
		return result;
	}

	/**
	 * scheduleStrategyDeal.jsp 页面上创建策略时调用 o.o，会先创建策略
	 * @param scheduleStrategy
	 * @throws Exception
	 */
	public void createScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
		String zkPath =	this.PATH_Strategy + "/"+ scheduleStrategy.getStrategyName();
		String valueString = this.gson.toJson(scheduleStrategy);
		if ( this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
		} else {
			throw new Exception("调度策略" + scheduleStrategy.getStrategyName() + "已经存在,如果确认需要重建，请先调用deleteMachineStrategy(String taskType)删除");
		}
	}

	public void updateScheduleStrategy(ScheduleStrategy scheduleStrategy)
			throws Exception {
		String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
		String valueString = this.gson.toJson(scheduleStrategy);
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
		} else {
			this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
		}

	}
	public void deleteMachineStrategy(String taskType) throws Exception {
		deleteMachineStrategy(taskType,false);
	}

	/**
	 * 暂停策略执行
	 * @param strategyName
	 * @throws Exception
	 */
    public void pause(String strategyName) throws Exception{
    	ScheduleStrategy strategy = this.loadStrategy(strategyName);
    	strategy.setSts(ScheduleStrategy.STS_PAUSE);
    	this.updateScheduleStrategy(strategy);
	}

	/**
	 * 回复策略执行
	 * @param strategyName
	 * @throws Exception
	 */
	public void resume(String strategyName) throws Exception{
    	ScheduleStrategy strategy = this.loadStrategy(strategyName);
    	strategy.setSts(ScheduleStrategy.STS_RESUME);
    	this.updateScheduleStrategy(strategy);		
	}
	
	public void deleteMachineStrategy(String taskType,boolean isForce) throws Exception {
		String zkPath = this.PATH_Strategy + "/" + taskType;
		if(isForce == false && this.getZooKeeper().getChildren(zkPath,null).size() >0){
			throw new Exception("不能删除"+ taskType +"的运行策略，会导致必须重启整个应用才能停止失去控制的调度进程。" +
					"可以先清空IP地址，等所有的调度器都停止后再删除调度策略");
		}
		ZKTools.deleteTree(this.getZooKeeper(),zkPath);
	}

	/**
	 * 查询zkManager roopath下的/strategy的所有子节点的data 即 strategy
	 * @return
	 * @throws Exception
	 */
	public List<ScheduleStrategy> loadAllScheduleStrategy() throws Exception {
		List<ScheduleStrategy> result = new ArrayList<ScheduleStrategy>();
		List<String> names = this.getZooKeeper().getChildren(this.PATH_Strategy,false);
		Collections.sort(names);
		for(String name:names){
			result.add(this.loadStrategy(name));
		}
		return result;
	}
	/**
	 * 注册ManagerFactory，过滤strategy
	 * 看工厂是否有UUID，无则生成，创建factory节点下创建工厂的UUID节点；有，则判断是否存在节点，没有则创建
	 * strategy是否符合该工厂，符合则确保在strategy节点下有该工厂的UUID，不符合则加入到result中
	 *
	 * 需要修改方法名称0.0
	 * @param managerFactory
	 * @return 需要全部注销的调度，例如当IP不在列表中
	 * @throws Exception
	 */
	public List<String> registerManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception{
		
		if(managerFactory.getUuid() == null){
			String uuid = managerFactory.getIp() +"$" + managerFactory.getHostName() +"$"+ UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
//			uuid = 192.168.35.165$liuguobin.local$7255A87318424E1683145E6D1ED856EA
//			/{root}/factory/ip$hostname$UUID$sequential
//			创建管理工厂的节点信息，此节点是临时顺序的，此版本的节点的子节点
			String zkPath = this.PATH_ManagerFactory + "/" + uuid +"$";
			zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
//			zkPath 不是没有变化吗？有变化，后缀一个sequential
//			 third_children0000000002
//			zkPath = 192.168.35.165$liuguobin.local$7255A87318424E1683145E6D1ED856EA$0000000250
//			uuid = 192.168.35.165$liuguobin.local$7255A87318424E1683145E6D1ED856EA$00000002501
			managerFactory.setUuid(zkPath.substring(zkPath.lastIndexOf("/") + 1));
		}else{
			String zkPath = this.PATH_ManagerFactory + "/" + managerFactory.getUuid();
			if(this.getZooKeeper().exists(zkPath, false)==null){
				zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);			
			}
		}

		return getStrategies(managerFactory);
	}

	/**
	 * 过滤策略节点下的所有策略，若策略设置的ip符合该调度器工行ip，则在策略节点下创建该工厂UUID节点。
	 * 若策略不符合，且策略下已经有了该工厂的节点，则将策略节点的名称添加到列表中返回，
	 * 因为需要停止策略，重新分配调度工厂
	 *
	 * 需要修改名字 0.0
	 * @param managerFactory
	 * @return
	 * @throws Exception
	 */
	private List<String> getStrategies(TBScheduleManagerFactory managerFactory) throws Exception {
		List<String> result = new ArrayList<String>();
		//过滤所有的strategy，若状态为暂停，或IP与跟ManagerFactory无关系，则删除
		for(ScheduleStrategy scheduleStrategy:loadAllScheduleStrategy()){
			boolean isFind = false;
			//不是暂停状态或并且IP范围不为空
			if(!ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(scheduleStrategy.getSts()) &&  scheduleStrategy.getIPList() != null){
				for(String ip:scheduleStrategy.getIPList()){
					//ip是本地地址，或者等于调度器工厂ip或域名
					if(ip.equals("127.0.0.1") || ip.equalsIgnoreCase("localhost") || ip.equals(managerFactory.getIp())|| ip.equalsIgnoreCase(managerFactory.getHostName())){
						//添加可管理TaskType
						String zkPath =	this.PATH_Strategy+"/"+ scheduleStrategy.getStrategyName()+ "/"+ managerFactory.getUuid();
						if(this.getZooKeeper().exists(zkPath, false)==null){
							zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);
//							{"strategyName":"testStrategy","uuid":"192.168.35.165$liuguobin.local$7255A87318424E1683145E6D1ED856EA$0000000250","requestNum":4,"currentNum":0,"message":""}
						}
						isFind = true;
						break;
					}
				}
			}
			if(isFind == false){//清除原来注册的Factory
				String zkPath =	this.PATH_Strategy+"/"+ scheduleStrategy.getStrategyName()+ "/"+ managerFactory.getUuid();
				if(this.getZooKeeper().exists(zkPath, false)!=null){
					ZKTools.deleteTree(this.getZooKeeper(), zkPath);
					result.add(scheduleStrategy.getStrategyName());
				}
			}
		}
		return result;
	}

	/**
	 * 注销服务，停止调度
	 * 删除{root}/strategy路径下的所有子节点
	 * 注意带有uuid版本好
	 * @param managerFactory
	 * @return
	 * @throws Exception
	 */
	public void unRregisterManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception{
		for(String taskName:this.getZooKeeper().getChildren(this.PATH_Strategy,false)){
			String zkPath =	this.PATH_Strategy+"/"+taskName+"/" + managerFactory.getUuid();
			if(this.getZooKeeper().exists(zkPath, false)!=null){
				ZKTools.deleteTree(this.getZooKeeper(), zkPath);
			}
		}
	}

	/**
	 * 根据策略名称+管理工厂的UUID，确定当前工厂的策略节点，获取节点值，
	 * 若节点值，不为空，反序列化成ScheduleStrategyRuntime
	 * 节点值为空，则新建一个
	 * 运行时的ScheduleStrategy，bean
	 * requestNum是该server分配的线程组数，是要求的数，并不是实际运行的线程组数//初始值为 0
     *
	 * strategyName就是策略的名字
	 * @param strategyName
	 * @param uuid
	 * @return
	 * @throws Exception
	 */
	public ScheduleStrategyRuntime loadScheduleStrategyRuntime(String strategyName, String uuid) throws Exception{
		String zkPath =	this.PATH_Strategy +"/"+strategyName+"/"+uuid;
		ScheduleStrategyRuntime result = null;
		if(this.getZooKeeper().exists(zkPath, false) !=null){
			byte[] value = this.getZooKeeper().getData(zkPath,false,null);
			if (value != null) {
				String valueString = new String(value);
				logger.info(valueString);
				result = (ScheduleStrategyRuntime) this.gson.fromJson(valueString, ScheduleStrategyRuntime.class);
				if(null==result){
					throw new Exception("gson 反序列化异常,对象为null");
				}
				if(null==result.getStrategyName()){
					throw new Exception("gson 反序列化异常,策略名字为null");
				}
				if(null==result.getUuid()){
					throw new Exception("gson 反序列化异常,uuid为null");
				}
			}else{
//				第一次遍历会走这一步，因为之前只创建了节点，没有设值
				result = new ScheduleStrategyRuntime();
				result.setStrategyName(strategyName);
				result.setUuid(uuid);
				result.setRequestNum(0);
				result.setMessage("");
			}
		}
		return result;
	}
	
	/**
	 * 装载所有的策略运行状态
	 * @return
	 * @throws Exception
	 */
	public List<ScheduleStrategyRuntime> loadAllScheduleStrategyRuntime() throws Exception{
		List<ScheduleStrategyRuntime> result = new ArrayList<ScheduleStrategyRuntime>();
		String zkPath =	this.PATH_Strategy;
		for(String taskType: this.getZooKeeper().getChildren(zkPath, false)){
			for(String uuid:this.getZooKeeper().getChildren(zkPath+"/"+taskType,false)){
				result.add(loadScheduleStrategyRuntime(taskType,uuid));
			}
		}
		return result;
	}

	/***
	 *
	 * 遍历strategy节点下的所有策略
	 * 若节点下有相应的工广节点存在，即能分配给该工厂执行的，若是没有设值，则返回一个初始化的
     * 返回存的的ScheduleStrategyRuntime列表
	 *
	 *
	 * @param managerFactoryUUID
	 * @return
	 * @throws Exception
	 */
	public List<ScheduleStrategyRuntime> loadAllScheduleStrategyRuntimeByUUID(String managerFactoryUUID) throws Exception{
		List<ScheduleStrategyRuntime> result = new ArrayList<ScheduleStrategyRuntime>();
//		/{$rootPath}/strategy/noPay24FrontTask-strategy
//		这里命名有问题，获取的是策略名称，所以应该命名为strategyNameList，遍历的是strategyName
		List<String> strategyNameList =  this.getZooKeeper().getChildren(this.PATH_Strategy, false);
		Collections.sort(strategyNameList);
		for(String strategyName:strategyNameList){
			if(this.getZooKeeper().exists(this.PATH_Strategy+"/"+strategyName+"/"+managerFactoryUUID, false) !=null){
				result.add(loadScheduleStrategyRuntime(strategyName,managerFactoryUUID));
			}
		}
		return result;
	}

    /**
     * 查询某策略下的所有factory的运行时的ScheduleStrategy
     * @param strategyName
     * @return
     * @throws Exception
     */
	public List<ScheduleStrategyRuntime> loadAllScheduleStrategyRuntimeByTaskType(String strategyName) throws Exception{
		List<ScheduleStrategyRuntime> result = new ArrayList<ScheduleStrategyRuntime>();
		String zkPath =	this.PATH_Strategy;
		if(this.getZooKeeper().exists(zkPath+"/"+strategyName, false)==null){
			return result;
		}
		List<String> uuidList = this.getZooKeeper().getChildren(zkPath + "/" + strategyName, false);
		//排序
		Collections.sort(uuidList,new Comparator<String>(){
			public int compare(String u1, String u2) {
				return u1.substring(u1.lastIndexOf("$") + 1).compareTo(
						u2.substring(u2.lastIndexOf("$") + 1));
			}
		});
		
		for (String uuid :uuidList) {
			result.add(loadScheduleStrategyRuntime(strategyName,uuid));
		}
		return result;
	}

	/**
	 *
	 * 各个策略下的工厂节点存的值就是策略名称，工厂UUID，最多能有的线程组数
	 * 更新策略下工厂的数据-最多可以生成的线程组数量
	 * @param strategyName
	 * @param managerFactoryUUID
	 * @param requestNum 要求的线程组数，可能用不了那么多
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void updateStrategyRuntimeRequestNum(String strategyName, String managerFactoryUUID, int requestNum) throws Exception{
		String zkPath =	this.PATH_Strategy +"/"+strategyName+"/" + managerFactoryUUID;
		ScheduleStrategyRuntime result = null;
		if(this.getZooKeeper().exists(zkPath, false) !=null){
			result = this.loadScheduleStrategyRuntime(strategyName,managerFactoryUUID);
		} else {
			result = new ScheduleStrategyRuntime();
			result.setStrategyName(strategyName);
			result.setUuid(managerFactoryUUID);
			result.setRequestNum(requestNum);
			result.setMessage("");
		}
		result.setRequestNum(requestNum);
		String valueString = this.gson.toJson(result);	
		this.getZooKeeper().setData(zkPath,valueString.getBytes(),-1);
	}
	/**
	 * 更新调度过程中的信息
	 * @param strategyName
	 * @param manangerFactoryUUID
	 * @param message
	 * @throws Exception
	 */
	public void updateStrategyRunntimeErrorMessage(String strategyName,String manangerFactoryUUID,String message) throws Exception{
		String zkPath =	this.PATH_Strategy +"/"+strategyName+"/" + manangerFactoryUUID;
		ScheduleStrategyRuntime result = null;
		if(this.getZooKeeper().exists(zkPath, false) !=null){
			result = this.loadScheduleStrategyRuntime(strategyName,manangerFactoryUUID);
		} else {
			result = new ScheduleStrategyRuntime();
			result.setStrategyName(strategyName);
			result.setUuid(manangerFactoryUUID);
			result.setRequestNum(0);
		}
		result.setMessage(message);
		String valueString = this.gson.toJson(result);	
		this.getZooKeeper().setData(zkPath,valueString.getBytes(),-1);
		}

	/**
	 * 机器管理更新 页面 0.0
	 * @param uuid
	 * @param isStart
	 * @throws Exception
	 */
	public void updateManagerFactoryInfo(String uuid,boolean isStart) throws Exception {
		String zkPath = this.PATH_ManagerFactory + "/" + uuid;
		if(this.getZooKeeper().exists(zkPath, false)==null){
			throw new Exception("任务管理器不存在:" + uuid);
		}
		this.getZooKeeper().setData(zkPath,Boolean.toString(isStart).getBytes(),-1);
	}

	/**
	 * 查询节点managefactory的节点data，返回新建的ManagerFactoryInfo 管loadManagerFactoryInfo理工厂信息
	 * 若是无值，设start为true
	 * 有值保持
	 * @param uuid
	 * @return
	 * @throws Exception
	 */
	public ManagerFactoryInfo loadManagerFactoryInfo(String uuid) throws Exception {
		String zkPath = this.PATH_ManagerFactory + "/" + uuid;
		if(this.getZooKeeper().exists(zkPath, false)==null){
			throw new Exception("任务管理器不存在:" + uuid);
		}
		byte[] value = this.getZooKeeper().getData(zkPath,false,null);
		ManagerFactoryInfo result = new ManagerFactoryInfo();
		result.setUuid(uuid);
//		第一次启动获取的是null，返回true
		if(value== null){
			result.setStart(true);
		}else{
			result.setStart(Boolean.parseBoolean(new String(value)));
		}
		return result;
	}
	
	/**
	 * 导入配置信息【目前支持baseTaskType和strategy数据】
	 * 
	 * @param config
	 * @param writer
	 * @param isUpdate
	 * @throws Exception
	 */
	public void importConfig(String config, Writer writer, boolean isUpdate)
			throws Exception {
		ConfigNode configNode = gson.fromJson(config, ConfigNode.class);
		if (configNode != null) {
			String path = configNode.getRootPath() + "/"
					+ configNode.getConfigType();
			ZKTools.createPath(getZooKeeper(), path, CreateMode.PERSISTENT, zkManager.getAcl());
			String y_node = path + "/" + configNode.getName();
			if (getZooKeeper().exists(y_node, false) == null) {
				writer.append("<font color=\"red\">成功导入新配置信息\n</font>");
				getZooKeeper().create(y_node, configNode.getValue().getBytes(),
						zkManager.getAcl(), CreateMode.PERSISTENT);
			} else if (isUpdate) {
				writer.append("<font color=\"red\">该配置信息已经存在，并且强制更新了\n</font>");
				getZooKeeper().setData(y_node,
						configNode.getValue().getBytes(), -1);
			} else {
				writer.append("<font color=\"red\">该配置信息已经存在，如果需要更新，请配置强制更新\n</font>");
			}
		}
		writer.append(configNode.toString());
	}

	/**
	 * 输出配置信息【目前备份baseTaskType和strategy数据】
	 * 
	 * @param rootPath
	 * @param writer
	 * @throws Exception
	 */
	public StringBuffer exportConfig(String rootPath, Writer writer)
			throws Exception {
		StringBuffer buffer = new StringBuffer();
		for (String type : new String[] { "baseTaskType", "strategy" }) {
			if (type.equals("baseTaskType")) {
				writer.write("<h2>基本任务配置列表：</h2>\n");
			} else {
				writer.write("<h2>基本策略配置列表：</h2>\n");
			}
			String bTTypePath = rootPath + "/" + type;
			List<String> fNodeList = getZooKeeper().getChildren(bTTypePath,
					false);
			for (int i = 0; i < fNodeList.size(); i++) {
				String fNode = fNodeList.get(i);
				ConfigNode configNode = new ConfigNode(rootPath, type, fNode);
				configNode.setValue(new String(this.getZooKeeper().getData(bTTypePath + "/" + fNode,false,null)));
				buffer.append(gson.toJson(configNode));
				buffer.append("\n");
				writer.write(configNode.toString());
			}
			writer.write("\n\n");
		}
		if (buffer.length() > 0) {
			String str = buffer.toString();
			return new StringBuffer(str.substring(0, str.length() - 1));
		}
		return buffer;
	}

	/**
	 * 判断当前版本的调度管理器对应的节点是否存在，不存在则直接抛异常，存在则构建ManagerFactoryInfo返回
	 * 第一次启动，当前版本的节点肯定不存在，所以返回的是null
	 * @return
	 * @throws Exception
	 */
	public List<ManagerFactoryInfo> loadAllManagerFactoryInfo() throws Exception {
		String zkPath = this.PATH_ManagerFactory;
		List<ManagerFactoryInfo> result = new ArrayList<ManagerFactoryInfo>();
		List<String> names = this.getZooKeeper().getChildren(zkPath,false);
		Collections.sort(names,new Comparator<String>(){
			public int compare(String u1, String u2) {
				return u1.substring(u1.lastIndexOf("$") + 1).compareTo(
						u2.substring(u2.lastIndexOf("$") + 1));
			}
		});
		for(String name:names){
			ManagerFactoryInfo info = new ManagerFactoryInfo();
			info.setUuid(name);
			byte[] value = this.getZooKeeper().getData(zkPath+"/"+name,false,null);
			if(value== null){
				info.setStart(true);
			}else{
				info.setStart(Boolean.parseBoolean(new String(value)));
			}
			result.add(info);
		}
		return result;
	}
	public void printTree(String path, Writer writer,String lineSplitChar)
			throws Exception {
		ZKTools.printTree(this.getZooKeeper(),path,writer,lineSplitChar);
	}
	public void deleteTree(String path) throws Exception{
		ZKTools.deleteTree(this.getZooKeeper(), path);
	}
	public ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZooKeeper();
	}
	public String getRootPath(){
		return this.zkManager.getRootPath();
	}
}
