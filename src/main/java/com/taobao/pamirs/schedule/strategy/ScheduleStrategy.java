package com.taobao.pamirs.schedule.strategy;

import org.apache.commons.lang.builder.ToStringBuilder;


/**
 * 调度策略
 * 在页面创建的策略，即是创建该对象。
 * 一个策略对应一个任务
 */
public class ScheduleStrategy {
	public enum Kind{Schedule,Java,Bean}
	/**
	 * 任务类型
	 */
	private String strategyName; //策略名称

	private String[] IPList;//IP地址 	127.0.0.1或者localhost会在所有机器上运行

	private int numOfSingleServer;//是单JVM最大线程数量，为什么不起作用，就是为了保证所有的任务必须都分配出去。
	/**
	 * 最大线程组数量	 所有服务器总共运行的最大数量
	 */
	private int assignNum;
	
	private Kind kind; //任务类型 可选类型：Schedule,Java,Bean 大小写敏感
	
	/**
	 * 任务名称
	 * 类名称，会带有$自定义参数0.0
	 */
	private String taskName; 
	
	private String taskParameter; // 任务参数
	
    /**
     * 服务状态: pause,resume
     */
    private String sts = STS_RESUME;
	
    public static String STS_PAUSE="pause";
    public static String STS_RESUME="resume";

	
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
	
	public String getStrategyName() {
		return strategyName;
	}

	public void setStrategyName(String strategyName) {
		this.strategyName = strategyName;
	}

	public int getAssignNum() {
		return assignNum;
	}

	public void setAssignNum(int assignNum) {
		this.assignNum = assignNum;
	}

	public String[] getIPList() {
		return IPList;
	}

	public void setIPList(String[] iPList) {
		IPList = iPList;
	}

	public void setNumOfSingleServer(int numOfSingleServer) {
		this.numOfSingleServer = numOfSingleServer;
	}

	public int getNumOfSingleServer() {
		return numOfSingleServer;
	}
	public Kind getKind() {
		return kind;
	}
	public void setKind(Kind kind) {
		this.kind = kind;
	}
	public String getTaskName() {
		return taskName;
	}
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public String getTaskParameter() {
		return taskParameter;
	}

	public void setTaskParameter(String taskParameter) {
		this.taskParameter = taskParameter;
	}

	public String getSts() {
		return sts;
	}

	public void setSts(String sts) {
		this.sts = sts;
	}	
}
