package com.taobao.pamirs.schedule.taskmanager;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 调度任务类型
 * 在页面进行配置的时候，会创建此对象，
 * 具体步骤是在
 * ScheduleDataManager4ZK.loadTaskTypeBaseInfo(任务名称，并不是任务className)
 * @author xuannan
 *
 */
public class ScheduleTaskType implements java.io.Serializable {
	private static transient Logger logger = LoggerFactory.getLogger(ScheduleTaskType.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 页面任务名称，不会包含$
	 */
	private String baseTaskType;
    /**
     * 向配置中心更新心跳信息的频率，页面可配置
     */
    private long heartBeatRate = 5*1000;//
    
    /**
     * 判断一个服务器死亡的周期。为了安全，至少是心跳周期的两倍以上，可配置
     */
    private long judgeDeadInterval = 1*60*1000;//1分钟
    
    /**
     * 页面设置 当没有数据的时候，休眠的时间
     * 
     */
    private int sleepTimeNoData = 500;
    
    /**
     * 页面设置的 每次处理完数据后休眠时间 会在processor中 加载任务数据的时候，休眠
     */
    private int sleepTimeInterval = 0;
    
    /**
     * 每次获取数据的数量
     */
    private int fetchDataNumber = 500;
    
    /**
     * 在批处理的时候，每次处理的数据量
	 * 页面上配置任务的时候 填的 每次执行数量
	 */
    private int executeNumber =1;//只在bean实现IScheduleTaskDealMulti才生效

//	线程数0.0 有用吗
//	在类TBScheduleProcessorSleep 是需要启动足够threadNumber个线程，如果taskDealBean是single的子类，则只只会有一个
    private int threadNumber = 5;
    
    /**
     * 调度器类型、处理模式
     */
    private String processorType="SLEEP" ;
    /**
     * 允许执行的开始时间，一般都是页面上的正则表达式。不会为空
     */
    private String permitRunStartTime;
    /**
     * 页面上设置的允许执行的结束时间 //会在休眠时起作用 permitRunEndTime为空或者不等于 -1，页面也没给提示什么的0.0
     */
    private String permitRunEndTime;
    
    /**
     * 清除过期环境信息的时间间隔,以天为单位
     */
    private double expireOwnSignInterval = 1;
    
    /**
     * 处理任务的BeanName,
     */
    private String dealBeanName;
    /**
     * 任务bean的参数，由用户自定义格式的字符串
     */
    private String taskParameter;
    
    //任务类型：静态static,动态dynamic
    private String taskKind = TASKKIND_STATIC;
    
    public static String TASKKIND_STATIC="static";
    public static String TASKKIND_DYNAMIC="dynamic";
 
    
    /**
     * 任务项数组
     */
    private String[] taskItems;
    
    /**
     * 每个线程组能处理的最大任务项目书目
     */
    private int maxTaskItemsOfOneThreadGroup = 0;
    /**
     * 版本号
     */
    private long version;
    
    /**
     * 服务状态: pause,resume
     */
    private String sts = STS_RESUME;
	
    public static String STS_PAUSE="pause";
    public static String STS_RESUME="resume";
    
    public static String[] splitTaskItem(String str){
    	List<String> list = new ArrayList<String>();
		int start = 0;
		int index = 0;
    	while(index < str.length()){
    		if(str.charAt(index)==':'){
    			index = str.indexOf('}', index) + 1;//:之后的}的index
    			list.add(str.substring(start,index).trim());
//				去除 } 之后的 空格
    			while(index <str.length()){
    				if(str.charAt(index) ==' '){
    					index = index +1;
    				}else{
    					break;
    				}
    			}
    			index = index + 1; //跳过逗号
    			start = index;
    		}else if(str.charAt(index)==','){
    			list.add(str.substring(start,index).trim());
//				去除 ， 之后的 空格
    			while(index <str.length()){
    				if(str.charAt(index) ==' '){
    					index = index +1;
    				}else{
    					break;
    				}
    			}
    			index = index + 1; //跳过逗号
    			start = index;
    		}else{
    			index = index + 1;
    		}
    	}
    	if(start < str.length()){
    		list.add(str.substring(start).trim());
    	}
    	return (String[]) list.toArray(new String[0]);
     }
    
	public long getVersion() {
		return version;
	}
	public void setVersion(long version) {
		this.version = version;
	}
	
	public String getBaseTaskType() {
		return baseTaskType;
	}
	public void setBaseTaskType(String baseTaskType) {
		this.baseTaskType = baseTaskType;
	}
	public long getHeartBeatRate() {
		return heartBeatRate;
	}
	public void setHeartBeatRate(long heartBeatRate) {
		this.heartBeatRate = heartBeatRate;
	}

	public long getJudgeDeadInterval() {
		return judgeDeadInterval;
	}

	public void setJudgeDeadInterval(long judgeDeadInterval) {
		this.judgeDeadInterval = judgeDeadInterval;
	}

	public int getFetchDataNumber() {
		return fetchDataNumber;
	}

	public void setFetchDataNumber(int fetchDataNumber) {
		this.fetchDataNumber = fetchDataNumber;
	}

	public int getExecuteNumber() {
		return executeNumber;
	}

	public void setExecuteNumber(int executeNumber) {
		this.executeNumber = executeNumber;
	}

	public int getSleepTimeNoData() {
		return sleepTimeNoData;
	}

	public void setSleepTimeNoData(int sleepTimeNoData) {
		this.sleepTimeNoData = sleepTimeNoData;
	}

	public int getSleepTimeInterval() {
		return sleepTimeInterval;
	}

	public void setSleepTimeInterval(int sleepTimeInterval) {
		this.sleepTimeInterval = sleepTimeInterval;
	}

	public int getThreadNumber() {
		return threadNumber;
	}

	public void setThreadNumber(int threadNumber) {
		this.threadNumber = threadNumber;
	}

	public String getPermitRunStartTime() {
		return permitRunStartTime;
	}

	public String getProcessorType() {
		return processorType;
	}

	public void setProcessorType(String processorType) {
		this.processorType = processorType;
	}

	public void setPermitRunStartTime(String permitRunStartTime) {
		this.permitRunStartTime = permitRunStartTime;
		if(this.permitRunStartTime != null && this.permitRunStartTime.trim().length() == 0){
			this.permitRunStartTime = null;
		}	
	}

	public String getPermitRunEndTime() {
		return permitRunEndTime;
	}

	public double getExpireOwnSignInterval() {
		return expireOwnSignInterval;
	}
	public void setExpireOwnSignInterval(double expireOwnSignInterval) {
		this.expireOwnSignInterval = expireOwnSignInterval;
	}
	
	public String getDealBeanName() {
		return dealBeanName;
	}
	public void setDealBeanName(String dealBeanName) {
		this.dealBeanName = dealBeanName;
	}
	public void setPermitRunEndTime(String permitRunEndTime) {
		this.permitRunEndTime = permitRunEndTime;
		if(this.permitRunEndTime != null && this.permitRunEndTime.trim().length() ==0){
			this.permitRunEndTime = null;
		}	

	}

	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
	public void setTaskItems(String[] aTaskItems) {
		this.taskItems = aTaskItems;
	}
	public String[] getTaskItems() {
		return taskItems;
	}
	public void setSts(String sts) {
		this.sts = sts;
	}
	public String getSts() {
		return sts;
	}
	public void setTaskKind(String taskKind) {
		this.taskKind = taskKind;
	}
	public String getTaskKind() {
		return taskKind;
	}
	public void setTaskParameter(String taskParameter) {
		this.taskParameter = taskParameter;
	}
	public String getTaskParameter() {
		return taskParameter;
	}

	public int getMaxTaskItemsOfOneThreadGroup() {
		return maxTaskItemsOfOneThreadGroup;
	}

	public void setMaxTaskItemsOfOneThreadGroup(int maxTaskItemsOfOneThreadGroup) {
		this.maxTaskItemsOfOneThreadGroup = maxTaskItemsOfOneThreadGroup;
	}
	

//	自己测试
	public static void main(String[] args) {
		ScheduleTaskType scheduleTaskType = new ScheduleTaskType();
		String[] str = null;
//		str = scheduleTaskType.splitTaskItem("{a = b},{c = d}"); //{a = b}、{c = d}
//		str = scheduleTaskType.splitTaskItem("{a : b},{c : d}"); //{a : b}、{c : d}
		str = scheduleTaskType.splitTaskItem("{a : b,c,d},{a : e,f,g}"); //{a : b,c,d}、{a : e,f,g}
		str = scheduleTaskType.splitTaskItem("a:{a,b,c,d},a:{a,e,f,g}"); //a:{a,b,c,d}、a:{a,e,f,g}
//		str = scheduleTaskType.splitTaskItem("a : b,c : d"); // index out of range
//		str = scheduleTaskType.splitTaskItem("a : {1,5},a : {6,10}"); // a : {1,5}、a : {6,10}
//		str = scheduleTaskType.splitTaskItem("{1,5},{6,10}"); // {1、5}、{6、10}
//		str = scheduleTaskType.splitTaskItem("{1:5}{6:10}"); // {1:5}、6:10}
//		str = scheduleTaskType.splitTaskItem("{1:5} {6:10}"); // {1:5}、6:10}//说明，必须需要,
//		str = scheduleTaskType.splitTaskItem("{1:5} , {6:10}"); // {1:5}、{6:10}

//		所以页面设定taskItem应该有些限制，要么不用":" ，用，就要有"}"结尾，如果有"}"，则必须要用"，"分割，
// 只有这种情况的下的位于 ： 和 } 之中的 ， 才不被分割，其他的情况下都被分割。
//

		for(int i = 0; i < str.length; i ++) {
			System.out.println(str[i]);
		}
	}

//	void  SleepTimeInterval((T)IScheduleProcessor  tbScheduleProcessorSleep) {
	void sleepTimeInterval(TBScheduleProcessorSleep tbScheduleProcessorSleep) throws InterruptedException {
		// 在每次数据处理完毕后休眠固定的时间
			if (this.getSleepTimeInterval() > 0) {
				if (logger.isTraceEnabled()) {
					logger.trace("处理完一批数据后休眠："
							+ this.getSleepTimeInterval());
				}
				tbScheduleProcessorSleep.isSleeping = true;
				Thread.sleep(this.getSleepTimeInterval());
				tbScheduleProcessorSleep.isSleeping = false;

				if (logger.isTraceEnabled()) {
					logger.trace("处理完一批数据后休眠后恢复");
				}
			}
	}
}
