package com.taobao.pamirs.schedule.strategy;

/**
 * 策略任务
 */
public interface IStrategyTask {
   public void initialTaskParameter(String strategyName,String taskParameter) throws Exception;
   /**
    * 任务的关闭在这里
    */
   public void stop(String strategyName) throws Exception;
}
