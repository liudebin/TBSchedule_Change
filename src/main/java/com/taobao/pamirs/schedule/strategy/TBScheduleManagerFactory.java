package com.taobao.pamirs.schedule.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.tools.internal.xjc.reader.relaxng.RELAXNGInternalizationLogic;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.taobao.pamirs.schedule.ConsoleManager;
import com.taobao.pamirs.schedule.IScheduleTaskDeal;
import com.taobao.pamirs.schedule.ScheduleUtil;
import com.taobao.pamirs.schedule.taskmanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.taskmanager.TBScheduleManagerStatic;
import com.taobao.pamirs.schedule.zk.ScheduleDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ZKManager;

/**
 * 调度服务器构造器
 * 
 * @author xuannan
 * 初始调度框架的核心入口
 */
public class TBScheduleManagerFactory implements ApplicationContextAware {
//	用transient关键字标记的成员变量不参与序列化.
	protected static transient Logger logger = LoggerFactory.getLogger(TBScheduleManagerFactory.class);
	
	private Map<String,String> zkConfig;

	/**
	 * 管理zk的链接的初始化等和节点对比
	 */
	protected ZKManager zkManager;

	/**
	 * 是否启动调度管理，如果只是做系统管理，应该设置为false
	 */
	public boolean start = true;
	private int timerInterval = 2000;
	/**
	 * ManagerFactoryTimerTask上次执行的时间戳。<br/>
	 * zk环境不稳定，可能导致所有task自循环丢失，调度停止。<br/>
	 * 外层应用，通过jmx暴露心跳时间，监控这个tbschedule最重要的大循环。<br/>
	 */
	public volatile long timerTaskHeartBeatTS = System.currentTimeMillis();
	
	/**
	 * 调度配置中心客户端，接口，ScheduleStrategyDataManager4ZK，是唯一实现类
	 */
	private IScheduleDataManager scheduleDataManager;
	/**
	 * 调度策略配置中心客户端
	 */
	private ScheduleStrategyDataManager4ZK scheduleStrategyManager;

	/**
	 * IStrategyTask 的实现类TBScheduleManager，TBScheduleManagerStatic，TBScheduleManagerDynamic;
	 * 不同策略的任务调度，后两个继承了TBScheduleManager类，常用TBScheduleManagerStatic
	 *
	 * 什么时候把任务放进去的呢？0.0
	 * 只有在方法reRunScheduleServer中有put操作，后边再仔细看，将结果记录在这儿
	 */
	private Map<String,List<IStrategyTask>> managerMap = new ConcurrentHashMap<String, List<IStrategyTask>>();
	
	private ApplicationContext			applicationcontext;	
	private String uuid;
	private String ip;
	private String hostName;

	private Timer timer;
	private ManagerFactoryTimerTask timerTask;

	protected Lock  lock = new ReentrantLock();
    
	volatile String  errorMessage ="No config Zookeeper connect infomation";
	/**
	 * 初始化TBScheduleManagerFactory的线程
	 */
	private InitialThread initialThread;
	
	public TBScheduleManagerFactory() {
		this.ip = ScheduleUtil.getLocalIP();
		this.hostName = ScheduleUtil.getLocalHostName();
	}

	/**
	 * restart调用
	 * 通过指定zkConfig，初始化实例
	 * zkConfig 其实就是一个Properties文件的要保留的键值对,TBScheduleManagerFactory需要properties文件，就是为了配置Zookeeper的
	 * @throws Exception
	 */
	public void init() throws Exception {
		Properties properties = new Properties();
		for(Map.Entry<String,String> e: this.zkConfig.entrySet()){
			properties.put(e.getKey(),e.getValue());
		}
		this.init(properties);
	}

	/**
	 * 修改Zookeeper配置文件的时候，会调用这个方法，重新进行初始化，只有管理端会有此操作.
	 *
	 * @param p
	 * @throws Exception
	 */
	public void reInit(Properties p) throws Exception{
		if(this.start == true || this.timer != null || this.managerMap.size() >0){
			throw new Exception("调度器有任务处理，不能重新初始化");
		}
		logger.info("重新初始化TBSdheduleManagerFactory");
		this.init(p);
	}

	/**
	 * 传入的是Zookeeper配置信息
	 * @param p
	 * @throws Exception
	 */
	public void init(Properties p) throws Exception {
	    if(this.initialThread != null){
	    	this.initialThread.stopThread();
	    }
		this.lock.lock();//锁定，待到初始化线程对象启动后，再解锁，防止多线程中间数据异常，保持原子性。
		// 放大版的lazy init，因为要读取一些配置的东西，又不能影响其他bean的初始化，所以单独拿出一个线程初始化。
		try{
			this.scheduleDataManager = null;
			this.scheduleStrategyManager = null;
		    ConsoleManager.setScheduleManagerFactory(this);
		    if(this.zkManager != null){
				this.zkManager.close();
			}
			this.zkManager = new ZKManager(p);
			this.errorMessage = "Zookeeper connecting ......" + this.zkManager.getConnectStr();
//			单独创建一个线程对象，完成它自己的初始化工作，最终会调用 自身的 initialData
			initialThread = new InitialThread(this);
			initialThread.setName("TBScheduleManagerFactory-initialThread");
			initialThread.start();
		}finally{
			this.lock.unlock();
		}
	}
    
    /**
     * 在检测ZK状态正常后，会调用该方法，对TBScheduleManagerFactory进行数据初始化
	 * 在Spring对象创建完毕后，创建内部对象
     * @throws Exception
     */
	public void initialData() throws Exception{
		//初始化ZK的配置根路径信息，验证、设置根路径和版本号，
		this.zkManager.initial();
		//创建ScheduleDataManager4ZK对象，负责实时地维护调度时的ZK数据，
//		创建baseTaskType节点 /{$rootPath}/baseTaskType
		this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
//		创建ScheduleStategyDataManager4ZK对象，负责维护调度策略的ZK数据
//		创建调度器的策略路径 = /{root}/strategy 持久性路径
//		创建调度器的工厂路径 = /{root}/factory 持久性路径
		this.scheduleStrategyManager = new ScheduleStrategyDataManager4ZK(this.zkManager);
//		如果是管理端的话，就不会做下边的操作
//
//		如：注册为工厂类啊，分配策略等等。

		if (this.start) {
			// 注册调取器的管理工厂的对象节点，设置UUID，创建管理器工厂节点，过滤不可用调度策略等
//			创建节点 /{root}/factory/工厂的uuid
//			uuid = ip$hostname$UUID$sequential
//			在所有能使用到的strategy下边创建相应工厂节点
//				/{root}/strategy/策略名称/工厂的uuid
			this.scheduleStrategyManager.registerManagerFactory(this);
			if(timer == null){
				timer = new Timer("TBScheduleManagerFactory-Timer");
			}
			if(timerTask == null){
				timerTask = new ManagerFactoryTimerTask(this);
//				每隔2秒循环执行timerTask
				timer.schedule(timerTask, 2000,this.timerInterval);// task，延迟时间，间歇间隔
			}
		}
	}

	/**
	 * 创建调度服务器
	 * @return
	 * @throws Exception
	 */
	public IStrategyTask createStrategyTask(ScheduleStrategy strategy)
			throws Exception {
		IStrategyTask result = null;
		try{
			if(ScheduleStrategy.Kind.Schedule == strategy.getKind()){
//				Schedule类型默认使用TBSchedule框架的调度器。TBScheduleManagerStatic
				//task类型名称
				String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(strategy.getTaskName());
				String ownSign =ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());//任务名称中$+归属标志，是不是一个类名，多个服务都会有，然后做个区别
				result = new TBScheduleManagerStatic(this,baseTaskType,ownSign,scheduleDataManager);
			}else if(ScheduleStrategy.Kind.Java == strategy.getKind()){
			    result=(IStrategyTask)Class.forName(strategy.getTaskName()).newInstance();
			    result.initialTaskParameter(strategy.getStrategyName(),strategy.getTaskParameter());
			}else if(ScheduleStrategy.Kind.Bean == strategy.getKind()){
			    result=(IStrategyTask)this.getBean(strategy.getTaskName());
			    result.initialTaskParameter(strategy.getStrategyName(),strategy.getTaskParameter());
			}
		}catch(Exception e ){
			logger.error("strategy 获取对应的java or bean 出错,schedule并没有加载该任务,请确认" +strategy.getStrategyName(),e);
		}
		return result;
	}

	/**
	 * 刷新核心管理工厂---调度服务器
	 * @throws Exception
	 */
	public void refresh() throws Exception {
//		refresh 跟 init 隔离
//		同一线程也不会多次进入啊
		this.lock.lock();
		try {
			// 判断状态是否终止
			ManagerFactoryInfo stsInfo = null;
			boolean isException = false;
			try {
				//获取当前版本的管理工厂的节点信息
//				如果是调用factory.restart，节点依然存在
//				已经注册过该工厂，不然哪儿来的uuid
				stsInfo = this.getScheduleStrategyManager().loadManagerFactoryInfo(this.getUuid());
			} catch (Exception e) {
				isException = true;
				logger.error("获取服务器信息有误：uuid="+this.getUuid(), e);
			}
			if (isException == true) {
//				加载管理工厂节点信息，若抛异常，说明factory节点下不存在该工厂子节点。
// 				停止所有的调度任务，注销工厂
//				finally 重新注册管理工厂
				try {
					stopServer(null); // 停止所有的调度任务
					this.getScheduleStrategyManager().unRregisterManagerFactory(this);
				} finally {
					reRegisterManagerFactory();
				}
			} else if (stsInfo.isStart() == false) {
//				加载工厂信息失败，停止所有调度任务，注销核心管理工厂
				stopServer(null); // 停止所有的调度任务
				this.getScheduleStrategyManager().unRregisterManagerFactory(
						this);
			} else {
//				第一次会走
//				重新注册管理工厂
				reRegisterManagerFactory();
			}
		} finally {
			this.lock.unlock();
		}
	}

	//这才是重点 o.o
	public void reRegisterManagerFactory() throws Exception{
//		没有注册工厂就注册工厂，注册过，就不操作。
//		返回需要重新分配的策略（策略下有该工厂节点，但是策略不能分配给该工厂）
//		第一次启动，返回为0
		List<String> stopList = this.getScheduleStrategyManager().registerManagerFactory(this);
		for (String strategyName : stopList) {
			this.stopServer(strategyName);
		}
		//分配每个factory最多可以有多少线程组数
		this.assignScheduleServer();
		this.reRunScheduleServer();
	}
	/**
	 * 分配工厂为leader的任务项
	 * 获取工厂可执行的策略，如果是leader则重新分配任务项，更新策略下的所有工厂节点分配的线程组数
	 * @throws Exception
	 */
	public void assignScheduleServer() throws Exception{
//		根据factory的uuid，获取当前sever（factory）分配到的运行策略信息，每个适合的策略都对应一个(策略对应任务)
		for(ScheduleStrategyRuntime run: this.scheduleStrategyManager.loadAllScheduleStrategyRuntimeByUUID(this.uuid)){
//			根据策略名称，获取整个集群所有的server（factory)的运行策略信息，进而重新分配任务项
			List<ScheduleStrategyRuntime> factoryList = this.scheduleStrategyManager.loadAllScheduleStrategyRuntimeByTaskType(run.getStrategyName());
//			判断是否是leader_server（用来分配任务项等），即序号最小的，如果不是，继续遍历下个策略
			if(factoryList.size() == 0 || this.isLeader(this.uuid, factoryList) ==false){
				continue;
			}

			//如果是leader进行如下操作
//			/{￥rootPath}/strategy/noPay24FrontTask-Strategy
			ScheduleStrategy scheduleStrategy =this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
			//分配每个工厂上的线程组数量
			// 工厂个数、最大的线程组数
			int[] nums =  ScheduleUtil.assignThreadGroupNum(factoryList.size(), scheduleStrategy.getAssignNum(), scheduleStrategy.getNumOfSingleServer());
			for(int i=0;i<factoryList.size();i++){
				ScheduleStrategyRuntime factory = factoryList.get(i);
				//更新请求的服务器数量
				this.scheduleStrategyManager.updateStrategyRuntimeRequestNum(run.getStrategyName(),
						factory.getUuid(),nums[i]);
			}
		}
	}


	/**
	 * uuid后边的序号最小的是leader
	 * 192.168.61.230$liuguobin.local$5D6A9687D8E74F7DAC784760131986AF$0000000066 中的 0000000066
	 * @param uuid
	 * @param factoryList
	 * @return
	 */
	public boolean isLeader(String uuid, List<ScheduleStrategyRuntime> factoryList) {
		try {
			long no = Long.parseLong(uuid.substring(uuid.lastIndexOf("$") + 1));
			for (ScheduleStrategyRuntime server : factoryList) {
				if (no > Long.parseLong(server.getUuid().substring(
						server.getUuid().lastIndexOf("$") + 1))) {
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			logger.error("判断Leader出错：uuid="+uuid, e);
			return true;
		}
	}

	/**
	 * 看看具体干嘛呢0.0
	 * 只有在这儿才会有创建任务相关吗？
	 * managerMap是在这初始化的，第一次进来的时候，list为空，所以要创建Task，task比较长，好好看看0.0
	 * 根据requestNum创建足够数量的TBScheduleManager
	 * 在这儿创建线程组？
	 * @throws Exception
	 */
	public void reRunScheduleServer() throws Exception{
//		根据factory的uuid，获取当前sever（factory）分配到的运行策略信息，每个适合的策略都对应一个(策略对应任务)
		for (ScheduleStrategyRuntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRuntimeByUUID(this.uuid)) {
//			获取属于当前任务类型管理器的所有策略管理器，一个strategy对应一个
//			key strategyName value List<Task>？
			List<IStrategyTask> list = this.managerMap.get(run.getStrategyName());
			if(list == null){
				list = new ArrayList<IStrategyTask>();
				this.managerMap.put(run.getStrategyName(),list);
			}
//			第一次肯定为0
//			如果获取的任务类型管理器（线程组）的数量 > 请求的数量，从list的尾部删除多余的
			while(list.size() > run.getRequestNum() && list.size() >0){
//				移除过多的任务
				IStrategyTask task  =  list.remove(list.size() - 1);
					try {
//						同时也停止对应的调度服务
						task.stop(run.getStrategyName());
					} catch (Throwable e) {
						logger.error("注销任务错误：strategyName=" + run.getStrategyName(), e);
					}
				}
		   //不足，增加调度器
//			根据策略名称，获取策略信息，即节点data
		   ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
//			根据运行策略信息的requestNum字段表示的数量，创建对应的策略管理器（TBScheduleManager),一般是一个?
		   while(list.size() < run.getRequestNum()){
			   IStrategyTask result = this.createStrategyTask(strategy);
			   if(null==result){
				   logger.error("strategy 对应的配置有问题。strategy name="+strategy.getStrategyName());
			   }
			   list.add(result);
		    }
		}
	}
	
	/**
	 * 终止一类任务
	 * 传入null，表示停止所有的任务调度
	 * 
	 * @throws Exception
	 */
	public void stopServer(String strategyName) throws Exception {
		if (strategyName == null) {
			String[] nameList = (String[])this.managerMap.keySet().toArray(new String[0]);
			for (String name : nameList) {
				for (IStrategyTask task : this.managerMap.get(name)) {
					try{
					  task.stop(strategyName);//0.0
					}catch(Throwable e){
					  logger.error("注销任务错误：strategyName="+strategyName,e);
					}
				}
				this.managerMap.remove(name);
			}
		} else {
			List<IStrategyTask> list = this.managerMap.get(strategyName);
			if(list != null){
				for(IStrategyTask task:list){
					try {
						task.stop(strategyName);
					} catch (Throwable e) {
						logger.error("注销任务错误：strategyName=" + strategyName, e);
					}
				}
				this.managerMap.remove(strategyName);
			}
			
		}			
	}
	/**
	 * 停止所有调度资源
	 */
	public void stopAll() throws Exception {
		try {
			lock.lock();
			this.setStart(false);
			if (this.initialThread != null) {
				this.initialThread.stopThread();
			}
			if (this.timer != null) {
				if (this.timerTask != null) {
					this.timerTask.cancel();
					this.timerTask = null;
				}
				this.timer.cancel();
				this.timer = null;
			}
			this.stopServer(null);
			if (this.zkManager != null) {
				this.zkManager.close();
			}
			if (this.scheduleStrategyManager != null) {
				try {
					ZooKeeper zk = this.scheduleStrategyManager.getZooKeeper();
					if (zk != null) {
						zk.close();
					}
				} catch (Exception e) {
					logger.error("stopAll zk getZooKeeper异常！",e);
				}
			}
			this.uuid = null;
			logger.info("stopAll 停止服务成功！");
		} catch (Throwable e) {
			logger.error("stopAll 停止服务失败：" + e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}
	/**
	 * 重启所有的服务
	 * @throws Exception
	 */
	public void reStart() throws Exception {
		try {
			if (this.timer != null) {
				if(this.timerTask != null){
					this.timerTask.cancel();
					this.timerTask = null;
				}
				this.timer.purge();
			}
			/**
			 * 停止所有任务调度
			 */
			this.stopServer(null);
			if (this.zkManager != null) {
				/**
				 * 关闭ZK管理器，断开与ZK的连接
				 */
				this.zkManager.close();
			}
			this.uuid = null;
			/**
			 * 重新调用TBScheduleManagerFactory的启动入口方法
			 */
			this.init();
		} catch (Throwable e) {
			logger.error("重启服务失败：" + e.getMessage(), e);
		}
    }
    public boolean isZookeeperInitialSucess() throws Exception{
    	return this.zkManager.checkZookeeperState();
    }
	public String[] getScheduleTaskDealList() {
		return applicationcontext.getBeanNamesForType(IScheduleTaskDeal.class);

	}

	/**
	 * 获取策略数据管理器，如果为空报异常
	 * @return
	 */
	public IScheduleDataManager getScheduleDataManager() {
		if(this.scheduleDataManager == null){
			throw new RuntimeException(this.errorMessage);
		}
		return scheduleDataManager;
	}
	public ScheduleStrategyDataManager4ZK getScheduleStrategyManager() {
		if(this.scheduleStrategyManager == null){
			throw new RuntimeException(this.errorMessage);
		}
		return scheduleStrategyManager;
	}

	public void setApplicationContext(ApplicationContext aApplicationcontext) throws BeansException {
		applicationcontext = aApplicationcontext;
	}

	public Object getBean(String beanName) {
		return applicationcontext.getBean(beanName);
	}
	public String getUuid() {
		return uuid;
	}

	public String getIp() {
		return ip;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getHostName() {
		return hostName;
	}

	public void setStart(boolean isStart) {
		this.start = isStart;
	}

	public void setTimerInterval(int timerInterval) {
		this.timerInterval = timerInterval;
	}
	public void setZkConfig(Map<String,String> zkConfig) {
		this.zkConfig = zkConfig;
	}
	public ZKManager getZkManager() {
		return this.zkManager;
	}
	public Map<String,String> getZkConfig() {
		return zkConfig;
	}
}

//有什么用0.0
class ManagerFactoryTimerTask extends java.util.TimerTask {
	private static transient Logger log = LoggerFactory.getLogger(ManagerFactoryTimerTask.class);
	TBScheduleManagerFactory factory;
	int count =0;

	public ManagerFactoryTimerTask(TBScheduleManagerFactory aFactory) {
		this.factory = aFactory;
	}

	/**
	 * 此任务是定时调度，Zookeeper链接状态正常，则刷新；不正常，则重启。
	 * 刷新：
	 *
	 * 重启：
	 * 	如果连接失败，关闭所有任务，重新连接服务器。0.0
	 */
	public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			/**
			 * 检查与ZK的链接状况，如果断开链接超过12秒，则执行重启
			 */
			if(this.factory.zkManager.checkZookeeperState() == false){
				if(count > 5){
				   log.error("Zookeeper连接失败，关闭所有的任务后，重新连接Zookeeper服务器......");
					/**
					 * 关闭当前task，关闭ZKManager，停止所有的任务，最后调用框架核心工厂的init()方法
					 */
				   this.factory.reStart();//0.0
				  
				}else{
				   count = count + 1;
				}
			}else{
				count = 0;
				/**
				 * 刷新调度服务器管理工厂TBScheduleManagerFactory，重新开始整个完整流程
				 */
			    this.factory.refresh();
			}

		}  catch (Throwable ex) {
			log.error(ex.getMessage(), ex);
		} finally {
		    factory.timerTaskHeartBeatTS = System.currentTimeMillis();
		}
	}
}

class InitialThread extends Thread{
	private static transient Logger log = LoggerFactory.getLogger(InitialThread.class);
	TBScheduleManagerFactory facotry;
	boolean isStop = false; //默认是false
	public InitialThread(TBScheduleManagerFactory aFactory){
		this.facotry = aFactory;
	}
	public void stopThread(){
		this.isStop = true;
	}

	/**
	 * Zookeeper配置正确，即连接上，才会进行初始化
	 */
	@Override
	public void run() {
		facotry.lock.lock();
		try {
			int count =0;
			/**
			 * zk != null && zk.getState() == States.CONNECTED
			 * 如果Zookeeper没有连接上，则执行睡眠等待
			 * 每重试50次，则会打一次错误日志
			 */
			while(facotry.zkManager.checkZookeeperState() == false){
				count = count + 1;
				if(count % 50 == 0){
					facotry.errorMessage = "Zookeeper connecting ......" + facotry.zkManager.getConnectStr() + " spendTime:" + count * 20 +"(ms)";
					log.error(facotry.errorMessage);
				}
				Thread.sleep(20);
				if(this.isStop ==true){
					return;
				}
			}
			/**
			 * 在单线程InitialThread中，执行启动任务，
			 * 执行此方法，factory在IOC容器
			 */
			facotry.initialData();
		} catch (Throwable e) {
			 log.error(e.getMessage(),e);
		}finally{
			facotry.lock.unlock();
		}

	}
	
}