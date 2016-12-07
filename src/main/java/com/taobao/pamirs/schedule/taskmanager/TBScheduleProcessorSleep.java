package com.taobao.pamirs.schedule.taskmanager;

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.IScheduleTaskDeal;
import com.taobao.pamirs.schedule.IScheduleTaskDealMulti;
import com.taobao.pamirs.schedule.IScheduleTaskDealSingle;
import com.taobao.pamirs.schedule.TaskItemDefine;

/**
 * 任务调度器，在TBScheduleManager的管理下实现多线程数据处理
 * @author xuannan
 *
 * @param <T>
 */
class TBScheduleProcessorSleep<T> implements IScheduleProcessor,Runnable {
	
	private static transient Logger logger = LoggerFactory.getLogger(TBScheduleProcessorSleep.class);
	final  LockObject   m_lockObject = new LockObject();

	/**
	 * 线程列表，写时复制列表
	 * 会加入多少线程，怎么移除
	 * 会加入tasktype中设定的线程数
	 * 当停止队列调度的时候，会移除，在本类的run方法中
	 */
	List<Thread> threadList =  new CopyOnWriteArrayList<Thread>();
	/**
	 * 任务管理器
	 */
	protected TBScheduleManager scheduleManager;
	/**
	 * 任务类型
	 */
	ScheduleTaskType taskTypeInfo;
	
	/**
	 * 任务处理的接口类，即我们用到的job其实就是查询的真实类
	 */
	protected IScheduleTaskDeal<T> taskDealBean;
		
	/**
	 * 当前任务队列的版本号
	 */
	protected long taskListVersion = 0;
	final Object lockVersionObject = new Object();
	final Object lockRunningList = new Object();

//	这个list是job中查询到的业务数据，即真正的Job中的selectTask返回的数据0.0
//	在 loadScheduleData，调用具体dealbean，即我们自己写的任务类，根据分配到的任务项，获取具体的数据。
	protected List<T> taskList = new CopyOnWriteArrayList<T>();

	/**
	 * 是否可以批处理
	 */
	boolean isMutilTask = false;
	
	/**
	 * 是否已经获得终止调度信号
	 */
	boolean isStopSchedule = false;// 用户停止队列调度
	boolean isSleeping = false;
	
	StatisticsInfo statisticsInfo;

	/**
	 * 创建一个调度处理器，并执行
	 * 根据任务页面配置的线程数，启动n个线程，若继承的是single 就只启动一个
	 *
	 * @param aManager
	 * @param aTaskDealBean
	 * @param aStatisticsInfo
	 * @throws Exception
	 */
	public TBScheduleProcessorSleep(TBScheduleManager aManager,
			IScheduleTaskDeal<T> aTaskDealBean,	StatisticsInfo aStatisticsInfo) throws Exception {
		this.scheduleManager = aManager;
		this.statisticsInfo = aStatisticsInfo;
		this.taskTypeInfo = this.scheduleManager.getTaskTypeInfo();
		this.taskDealBean = aTaskDealBean;
		if (this.taskDealBean instanceof IScheduleTaskDealSingle<?>) {
			if (taskTypeInfo.getExecuteNumber() > 1) {
				taskTypeInfo.setExecuteNumber(1);
			}
			isMutilTask = false;
		} else {
			isMutilTask = true;
		}
		if (taskTypeInfo.getFetchDataNumber() < taskTypeInfo.getThreadNumber() * 10) {
			logger.warn("参数设置不合理，系统性能不佳。【每次从数据库获取的数量fetchnum】 >= 【线程数量threadnum】 *【最少循环次数10】 ");
		}
//		在这是是要启动
		for (int i = 0; i < taskTypeInfo.getThreadNumber(); i++) {
			this.startThread(i);
		}
	}

	/**
	 * 需要注意的是，调度服务器从配置中心注销的工作，必须在所有线程退出的情况下才能做
	 * @throws Exception
	 */
	public void stopSchedule() throws Exception {
		// 设置停止调度的标志,调度线程发现这个标志，执行完当前任务后，就退出调度
		this.isStopSchedule = true;
		//清除所有未处理任务,但已经进入处理队列的，需要处理完毕
		this.taskList.clear();
	}

	private void startThread(int index) {
		Thread thread = new Thread(this);
		threadList.add(thread);
		String threadName = this.scheduleManager.getScheduleServer().getTaskType()+"-" 
				+ this.scheduleManager.getCurrentSerialNumber() + "-exe"
				+ index;
		thread.setName(threadName);
		thread.start();
	}

	/**
	 * 返回一个查询到的任务数据
	 * @return
	 */
	public synchronized Object getScheduleTask() {
		if (this.taskList.size() > 0)
			return this.taskList.remove(0);  // 按正序处理

		return null;
	}

	/**
	 * 返回多个任务
	 * 上限是任务列表size和每次任务执行的数量
	 *
	 * @return
	 */
	public synchronized Object[] getScheduleTaskIdMulti() {
		if (this.taskList.size() == 0){
			return null;
		}
		int size = taskList.size() > taskTypeInfo.getExecuteNumber() ? taskTypeInfo.getExecuteNumber()
				: taskList.size();
		       
		Object[] result = null;
		if(size >0){
			result =(Object[])Array.newInstance(this.taskList.get(0).getClass(),size);
		}
		for(int i=0;i<size;i++){
			result[i] = this.taskList.remove(0);  // 按正序处理
		}
		return result;
	}

	public void clearAllHasFetchData() {
		this.taskList.clear();
	}
	public boolean isDealFinishAllData() {
		return this.taskList.size() == 0 ;
	}
	
	public boolean isSleeping(){
    	return this.isSleeping;
    }

	/**
	 * 调度具体的任务，获取调度的数据
	 * 根据当前的任务项，调用具体的IScheduleTaskDeal，执行具体实现的查询，获取数据，赋值给taskList
	 * 更新最后一次取数据时间 lastFetchDataTime
	 * @return
	 */
	protected int loadScheduleData() {
		try {
//           在每次数据处理完毕后休眠固定的时间
//			是一批数据，一批又多少呢？ 0.0
			if (this.taskTypeInfo.getSleepTimeInterval() > 0) {
				if(logger.isTraceEnabled()){
					logger.trace("处理完一批数据后休眠：" + this.taskTypeInfo.getSleepTimeInterval());
				}
				this.isSleeping = true;
			    Thread.sleep(taskTypeInfo.getSleepTimeInterval());
			    this.isSleeping = false;
			    
				if(logger.isTraceEnabled()){
					logger.trace("处理完一批数据后休眠后恢复");
				}
			}
			
			List<TaskItemDefine> taskItems = this.scheduleManager.getCurrentScheduleTaskItemList();
			// 根据队列信息查询需要调度的数据，然后增加到任务列表中
			if (taskItems.size() > 0) {
				List<TaskItemDefine> tmpTaskList= new ArrayList<TaskItemDefine>();
				synchronized(taskItems){
					for (TaskItemDefine taskItemDefine : taskItems) {
						tmpTaskList.add(taskItemDefine);
					}
				}
//				在这儿调用具体的任务类，查询要处理的数据或任务
				List<T> tmpList = this.taskDealBean.selectTasks(
						taskTypeInfo.getTaskParameter(),
						scheduleManager.getScheduleServer().getOwnSign(),
						this.scheduleManager.getTaskItemCount(), tmpTaskList,
						taskTypeInfo.getFetchDataNumber());
				scheduleManager.getScheduleServer().setLastFetchDataTime(new Timestamp(scheduleManager.scheduleCenter.getSystemTime()));
				if(tmpList != null){
				   this.taskList.addAll(tmpList);
				}
			} else {
				if(logger.isTraceEnabled()){
//					不应该打日志，没有渠道相应的taskItem吗 0.0
					logger.trace("没有获取到需要处理的数据队列");
				}
			}
			addFetchNum(taskList.size(),"TBScheduleProcessor.loadScheduleData");
			return this.taskList.size();
		} catch (Throwable ex) {
			logger.error("Get tasks error.", ex);
		}
		return 0;
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "static-access" })
	/**
	 * 当新建对象的时候，会直接启动线程，运行该方法。
	 * 如果是停止调度，移除该线程，通知其他线程，如果是最后一个了，则删除该节点
	 *
	 * 在这儿调用的具体的taskDealBean类执行的execute方法，即对数据的处理。
	 */
	public void run(){
	      try {
	        long startTime =0;
	        while(true){
				this.m_lockObject.addThread();
	          	Object executeTask;
	          	while (true) {
//					如果是停止标志，则移除当前线程，如果线程列表为零后，就注销该服务，退出。
					if(this.isStopSchedule == true){//停止队列调度
						this.m_lockObject.realseThread();
						this.m_lockObject.notifyOtherThread();//通知所有的休眠线程
						synchronized (this.threadList) {
							this.threadList.remove(Thread.currentThread());
							if(this.threadList.size()==0){
//								从任务节点、server节点下删除这个server的相应节点
								this.scheduleManager.unRegisterScheduleServer();
							}
						}
						return;
					}

					//加载调度任务
					if(this.isMutilTask == false){
						executeTask = this.getScheduleTask();
					}else{
						executeTask = this.getScheduleTaskIdMulti();
					}
//					第一次执行，当需要需要执行的任务数据为空时
					if(executeTask == null){
						break;
					}

//					0.0
//					mutilTask 是要能批量处理的，so，什么情况下是能批量处理的呢
					try {//运行相关的程序
					  	startTime =scheduleManager.scheduleCenter.getSystemTime();
						if (this.isMutilTask == false) {
							/**
							 * 执行execute方法，调用handleTask方法
							 * 使用TBSchedule要具体实现的
							 */
							if (((IScheduleTaskDealSingle) this.taskDealBean).execute(executeTask,scheduleManager.getScheduleServer().getOwnSign()) == true) {
								addSuccessNum(1, scheduleManager.scheduleCenter.getSystemTime() - startTime,
											"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
							} else {
								addFailNum(1, scheduleManager.scheduleCenter.getSystemTime() - startTime,
										"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
							}
						} else {
							if (((IScheduleTaskDealMulti) this.taskDealBean).execute((Object[]) executeTask,scheduleManager.getScheduleServer().getOwnSign()) == true) {
								addSuccessNum(((Object[]) executeTask).length,scheduleManager.scheduleCenter.getSystemTime() - startTime,
										"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
							} else {
								addFailNum(((Object[]) executeTask).length,scheduleManager.scheduleCenter.getSystemTime() - startTime,
										"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
							}
						}
					}catch (Throwable ex) {
						if (this.isMutilTask == false) {
							addFailNum(1,scheduleManager.scheduleCenter.getSystemTime()- startTime,
									"TBScheduleProcessor.run");
						} else {
							addFailNum(((Object[]) executeTask).length, scheduleManager.scheduleCenter.getSystemTime() - startTime,
									"TBScheduleProcessor.run");
						}
						logger.warn("Task :" + executeTask + " 处理失败", ex);
					}

	          	}

//	          	如果不是最后一个线程，直接休眠。

	          //当前队列中所有的任务都已经完成了。
	            if(logger.isTraceEnabled()){
				   logger.trace(Thread.currentThread().getName() +"：当前运行线程数量:" +this.m_lockObject.count());
			    }

//				释放一个线程数量
//				如果是最后一个线程，则再加载下任务数据，若加载到则唤醒其他线程
				if (this.m_lockObject.realseThreadButNotLast() == false) {
					int size = 0;
					Thread.currentThread().sleep(100);
					startTime =scheduleManager.scheduleCenter.getSystemTime();

					/*
					* 调度具体的任务，获取调度的数据，会根据设
					* 根据当前的任务项，调用具体的IScheduleTaskDeal，执行具体实现的查询，获取数据，赋值给taskList
					* 更新最后一次取数据时间 lastFetchDataTime
					*/
					size = this.loadScheduleData();


					//线程数减一，
					if (size > 0) {
						this.m_lockObject.notifyOtherThread();
					} else {
//						最后一个线程，如果isStopSchedule 为false，且当没有数据的时候继续执行，则不退出只休眠，否则退出调度，唤醒其他线程
						//判断当没有数据的是否，是否需要退出调度
						if (this.isStopSchedule == false && this.scheduleManager.isContinueWhenData()== true ){						 
							if(logger.isTraceEnabled()){
								   logger.trace("没有装载到数据，start sleep");
							}
							this.isSleeping = true;
						    Thread.currentThread().sleep(this.scheduleManager.getTaskTypeInfo().getSleepTimeNoData());
						    this.isSleeping = false;
						    
						    if(logger.isTraceEnabled()){
								   logger.trace("Sleep end");
							}
						}else{
							//没有数据，退出调度，唤醒所有沉睡线程（要退出都退出）
//							怎么退出的呢？在下边线程减一
							this.m_lockObject.notifyOtherThread();
						}
					}
					this.m_lockObject.realseThread();//线程数减一
				} else {// 将当前线程放置到等待队列中。直到有线程装载到了新的任务数据
					if(logger.isTraceEnabled()){
						   logger.trace("不是最后一个线程，sleep");
					}
					this.m_lockObject.waitCurrentThread();
				}
	        }
	      } catch (Throwable e) {
	    	  logger.error(e.getMessage(), e);
	      }
	}

	public void addFetchNum(long num, String addr) {
		
        this.statisticsInfo.addFetchDataCount(1);
        this.statisticsInfo.addFetchDataNum(num);
	}

	public void addSuccessNum(long num, long spendTime, String addr) {
        this.statisticsInfo.addDealDataSucess(num);
        this.statisticsInfo.addDealSpendTime(spendTime);
	}

	public void addFailNum(long num, long spendTime, String addr) {
      this.statisticsInfo.addDealDataFail(num);
      this.statisticsInfo.addDealSpendTime(spendTime);
	}
}
