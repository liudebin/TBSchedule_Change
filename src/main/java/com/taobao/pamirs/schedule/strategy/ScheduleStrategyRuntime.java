package com.taobao.pamirs.schedule.strategy;

import com.taobao.pamirs.schedule.strategy.ScheduleStrategy.Kind;

/**
 * strategy节点下的factory的data
 * 即策略分配到的工厂信息
 */
public class ScheduleStrategyRuntime {
	
	/**
	 * 任务类型
	 */
	String strategyName;
	String uuid;
	String ip;
	
	private Kind kind; 
	
	/**
	 * Schedule Name,Class Name、Bean Name
	 */
	private String taskName; 
	
	private String taskParameter;
	
	/**
	 * 最大线程组数量
	 */
	int	requestNum;
	/**
	 * 当前的任务数量 怎么一直是0
	 */
	int currentNum;
	 
	String message;
	
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public String getStrategyName() {
		return strategyName;
	}
	public void setStrategyName(String strategyName) {
		this.strategyName = strategyName;
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
	public int getRequestNum() {
		return requestNum;
	}
	public void setRequestNum(int requestNum) {
		this.requestNum = requestNum;
	}
	public int getCurrentNum() {
		return currentNum;
	}
	public void setCurrentNum(int currentNum) {
		this.currentNum = currentNum;
	}
	@Override
	public String toString() {
		return "ScheduleStrategyRuntime [strategyName=" + strategyName
				+ ", uuid=" + uuid + ", ip=" + ip + ", kind=" + kind
				+ ", taskName=" + taskName + ", taskParameter=" + taskParameter
				+ ", requestNum=" + requestNum + ", currentNum=" + currentNum
				+ ", message=" + message + "]";
	}
	

}
