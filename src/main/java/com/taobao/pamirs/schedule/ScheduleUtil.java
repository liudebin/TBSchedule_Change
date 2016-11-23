package com.taobao.pamirs.schedule;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * 调度处理工具类
 * @author xuannan
 *
 */
public class ScheduleUtil {
	public static String OWN_SIGN_BASE ="BASE";

	public static String getLocalHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "";
		}
	}

	public static int getFreeSocketPort() {
		try {
			ServerSocket ss = new ServerSocket(0);
			int freePort = ss.getLocalPort();
			ss.close();
			return freePort;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static String getLocalIP() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			return "";
		}
	}

	public static String transferDataToString(Date d){
		SimpleDateFormat DATA_FORMAT_yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return DATA_FORMAT_yyyyMMddHHmmss.format(d);
	}
	public static Date transferStringToDate(String d) throws ParseException{
		SimpleDateFormat DATA_FORMAT_yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return DATA_FORMAT_yyyyMMddHHmmss.parse(d);
	}
	public static Date transferStringToDate(String d,String formate) throws ParseException{
		SimpleDateFormat FORMAT = new SimpleDateFormat(formate);
        return FORMAT.parse(d);
	}
	public static String getTaskTypeByBaseAndOwnSign(String baseType,String ownSign){
		if(ownSign.equals(OWN_SIGN_BASE) == true){
			return baseType;
		}
		return baseType+"$" + ownSign;
	}

	/**
	 * 由taskType获取BaseTaskType
	 * @param taskType
	 * @return
	 */
	public static String splitBaseTaskTypeFromTaskType(String taskType){
		 if(taskType.indexOf("$") >=0){
			 return taskType.substring(0,taskType.indexOf("$"));
		 }else{
			 return taskType;
		 }
		 
	}
	public static String splitOwnsignFromTaskType(String taskType){
		 if(taskType.indexOf("$") >=0){
			 return taskType.substring(taskType.indexOf("$")+1);
		 }else{
			 return OWN_SIGN_BASE;
		 }
	}	
	
	/**
	 * 分配线程组数量，而不是任务数量，序号小于余数的server分配的线程组数量，都是商+1
	 * @param serverNum 总的服务器数量
	 * @param threadGroupNum 线程组数量，并不是某个任务的任务项数量
	 * @param maxNumOfOneServer 每个server最大线程组数量
//	 * @param maxNum 总的任务数量
	 * @return
	 */
	public static int[] assignThreadGroupNum(int serverNum, int threadGroupNum, int maxNumOfOneServer){
		int[] taskNums = new int[serverNum];
		int numOfSingle = threadGroupNum / serverNum;
		int otherNum = threadGroupNum % serverNum;

		/**
		 * 这一块起作用的话，会有可能导致任务项无法分配完毕，他的算法是有问题的。
		 */

		//20150323 删除, 任务分片保证分配到所有的线程组数上。 开始
//		if (maxNumOfOneServer >0 && numOfSingle >= maxNumOfOneServer) {
//			numOfSingle = maxNumOfOneServer;
//			otherNum = 0;
//		}
		//20150323 删除, 任务分片保证分配到所有的线程组数上。 结束
		for (int i = 0; i < taskNums.length; i++) {
			if (i < otherNum) {
				taskNums[i] = numOfSingle + 1;
			} else {
				taskNums[i] = numOfSingle;
			}
		}
		return taskNums;
	}
	private static String printArray(int[] items){
		String s="";
		for(int i=0;i<items.length;i++){
			if(i >0){s = s +",";}
			s = s + items[i];
		}
		return s;
	}
	public static void main(String[] args) {
		System.out.println(printArray(assignThreadGroupNum(1,10,0)));
		System.out.println(printArray(assignThreadGroupNum(2,10,0)));
		System.out.println(printArray(assignThreadGroupNum(3,10,0)));
		System.out.println(printArray(assignThreadGroupNum(4,10,0)));
		System.out.println(printArray(assignThreadGroupNum(5,10,0)));
		System.out.println(printArray(assignThreadGroupNum(6,10,0)));
		System.out.println(printArray(assignThreadGroupNum(7,10,0)));
		System.out.println(printArray(assignThreadGroupNum(8,10,0)));
		System.out.println(printArray(assignThreadGroupNum(9,10,0)));
		System.out.println(printArray(assignThreadGroupNum(10,10,0)));
		
		System.out.println("-----------------");
		
		System.out.println(printArray(assignThreadGroupNum(1,10,3)));
		System.out.println(printArray(assignThreadGroupNum(2,10,3)));
		System.out.println(printArray(assignThreadGroupNum(3,10,3)));
		System.out.println(printArray(assignThreadGroupNum(4,10,3)));
		System.out.println(printArray(assignThreadGroupNum(5,10,3)));
		System.out.println(printArray(assignThreadGroupNum(6,10,3)));
		System.out.println(printArray(assignThreadGroupNum(7,10,3)));
		System.out.println(printArray(assignThreadGroupNum(8,10,3)));
		System.out.println(printArray(assignThreadGroupNum(9,10,3)));
		System.out.println(printArray(assignThreadGroupNum(10,10,3)));
		
	}
}
