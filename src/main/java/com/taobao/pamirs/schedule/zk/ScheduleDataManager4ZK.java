package com.taobao.pamirs.schedule.zk;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.taobao.pamirs.schedule.ScheduleUtil;
import com.taobao.pamirs.schedule.TaskItemDefine;
import com.taobao.pamirs.schedule.taskmanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.taskmanager.ScheduleServer;
import com.taobao.pamirs.schedule.taskmanager.ScheduleTaskItem;
import com.taobao.pamirs.schedule.taskmanager.ScheduleTaskType;
import com.taobao.pamirs.schedule.taskmanager.ScheduleTaskTypeRunningInfo;

/**
 * 负责实时维护调度时的ZK数据
 * 任务和任务项的管理
 */
public class ScheduleDataManager4ZK implements IScheduleDataManager {
	private static transient Logger log = LoggerFactory.getLogger(ScheduleDataManager4ZK.class);
	private Gson gson ;
	private ZKManager zkManager;
	private String PATH_BaseTaskType; // /{root}/baseTaskType
	private String PATH_TaskItem = "taskItem";
	private String PATH_Server = "server";
	private long zkBaseTime = 0;
	private long loclaBaseTime = 0;

    /**
     * 不存在则创建tasktype的ZK路径 = /{root}/baseTaskType
     * 比较Zookeeper服务器时间和本地服务器的时间差
     * @param aZkManager
     * @throws Exception
     */
    public ScheduleDataManager4ZK(ZKManager aZkManager) throws Exception {
    	this.zkManager = aZkManager;
    	gson = new GsonBuilder().registerTypeAdapter(Timestamp.class,new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		this.PATH_BaseTaskType = this.zkManager.getRootPath() +"/baseTaskType";
		
		if (this.getZooKeeper().exists(this.PATH_BaseTaskType, false) == null) {
			ZKTools.createPath(getZooKeeper(),this.PATH_BaseTaskType, CreateMode.PERSISTENT, this.zkManager.getAcl());
		}
        compareZKTime();
	}

    /**
     * 通过ZK创建一个临时节点，根据其创建时间与本地时间对比，大过本地时间5秒（不应该是大小0.0），打印log错误日志
     * @throws Exception
     */
    private void compareZKTime() throws Exception {
        loclaBaseTime = System.currentTimeMillis();

        String tempPath = this.zkManager.getZooKeeper().create(this.zkManager.getRootPath() + "/systime",null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
        Stat tempStat = this.zkManager.getZooKeeper().exists(tempPath, false);
        zkBaseTime = tempStat.getCtime();
        ZKTools.deleteTree(getZooKeeper(), tempPath);

        if(Math.abs(this.zkBaseTime - this.loclaBaseTime) > 5000){
        	log.error("请注意，Zookeeper服务器时间与本地时间相差 ： " + Math.abs(this.zkBaseTime - this.loclaBaseTime) +" ms");
        }
    }

    public ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZooKeeper();
	}

	/**
	 * 创建任务节点，页面用到o.o
 	 */
	public void createBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
		if(baseTaskType.getBaseTaskType().indexOf("$") > 0){
			throw new Exception("调度任务" + baseTaskType.getBaseTaskType() +"名称不能包括特殊字符 $");
		}
		String zkPath =	this.PATH_BaseTaskType + "/"+ baseTaskType.getBaseTaskType();
		String valueString = this.gson.toJson(baseTaskType);
		if ( this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
		} else {
			throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "已经存在,如果确认需要重建，请先调用deleteTaskType(String baseTaskType)删除");
		}
	}

	/**
	 * 页面上会有调用 o.o
	 * @param baseTaskType
	 * @throws Exception
	 */
	public void updateBaseTaskType(ScheduleTaskType baseTaskType)
			throws Exception {
		if(baseTaskType.getBaseTaskType().indexOf("$") > 0){
			throw new Exception("调度任务" + baseTaskType.getBaseTaskType() +"名称不能包括特殊字符 $");
		}
		String zkPath =	this.PATH_BaseTaskType + "/"+ baseTaskType.getBaseTaskType();
		String valueString = this.gson.toJson(baseTaskType);
		if ( this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
		}else{ 
			this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
		}
		
	}


	public void initialRunningInfo4Dynamic(String baseTaskType, String ownSign)throws Exception {
		 String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
		 //清除所有的老信息，只有leader能执行此操作
		 String zkPath = this.PATH_BaseTaskType+"/"+ baseTaskType +"/" + taskType;
		 if(this.getZooKeeper().exists(zkPath, false) == null){
			 this.getZooKeeper().create(zkPath,null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
		 }
	}

	/**
	 * 删除并重新创建 /{$rootPath}/baseTaskType/{$baseTaskType}/{$taskType}/taskItem
	 * @param baseTaskType
	 * @param ownSign
	 * @param uuid
	 * @throws Exception
	 */
	public void initialRunningInfo4Static(String baseTaskType, String ownSign,String uuid)
			throws Exception {
		  
		 String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
		 //清除所有的老信息，只有leader能执行此操作
		 String zkPath = this.PATH_BaseTaskType+"/"+ baseTaskType +"/" + taskType+"/" + this.PATH_TaskItem;
		 try {
			 ZKTools.deleteTree(this.getZooKeeper(),zkPath);
		 } catch (Exception e) {
				//需要处理zookeeper session过期异常
				if (e instanceof KeeperException
						&& ((KeeperException) e).code().intValue() == KeeperException.Code.SESSIONEXPIRED.intValue()) {
					log.warn("delete : zookeeper session已经过期，需要重新连接zookeeper");
					zkManager.reConnection();
					ZKTools.deleteTree(this.getZooKeeper(),zkPath);
				}
		 }
		 //创建目录 /{$rootPath}/baseTaskType/{$baseTaskType}/{$taskType}/taskItem
		 this.getZooKeeper().create(zkPath,null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
		 //创建任务的所有item节点问题
		 this.createScheduleTaskItem(baseTaskType, ownSign,this.loadTaskTypeBaseInfo(baseTaskType).getTaskItems());
		 //标记信息初始化成功，设置任务的item接点数据为 uuid为scheduleServer的uuid，该server为leader
		 setInitialRunningInfoSucuss(baseTaskType,taskType,uuid);
	}

    /**
     * 设置分配前的初始数据：
     * 将任务的taskItem节点设置为 leader Server uuid
     * /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/taskItem
     * myTbScheduleJob1$192.168.61.230$0B183B50EC88403A8BB2C6B5C9254CC2$0000000103
     * @param baseTaskType
     * @param taskType
     * @param uuid
     * @throws Exception
     */
	public void setInitialRunningInfoSucuss(String baseTaskType, String taskType,String uuid) throws Exception{
		 String zkPath = this.PATH_BaseTaskType+"/"+ baseTaskType +"/" + taskType+"/" + this.PATH_TaskItem;
		 this.getZooKeeper().setData(zkPath, uuid.getBytes(),-1);
	}

	/**
	 * 判断任务的item节点设置的 uuid是否是leader 的 uuid
	 * @param baseTaskType
	 * @param ownSign
	 * @return
	 * @throws Exception
	 */
    public boolean isInitialRunningInfoSucuss(String baseTaskType, String ownSign) throws Exception{
    	String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
    	String leader = this.getLeader(this.loadScheduleServerNames(taskType));
    	String zkPath = this.PATH_BaseTaskType+"/"+ baseTaskType +"/" + taskType+"/"+ this.PATH_TaskItem;
		if(this.getZooKeeper().exists(zkPath, false) != null){
			byte[] curContent = this.getZooKeeper().getData(zkPath,false,null);
			if(curContent != null && new String(curContent).equals(leader)){
				return true;
			}
		}
		return false;
    }

    /**
     *
     * 设置任务下server节点的data为 reload=true，zookeeper同时会修改stat的版本号
     * @param taskType
     * @return
     * @throws Exception
     */
    public long updateReloadTaskItemFlag(String taskType) throws Exception{
    	String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType 
		        + "/" + taskType + "/" + this.PATH_Server;
//         /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/server
		Stat stat = this.getZooKeeper().setData(zkPath,"reload=true".getBytes(),-1);
		return stat.getVersion();

    }

    /**
     * 获取任务所有当下server的stat的列表
     * @param taskType
     * @return
     * @throws Exception
     */
    @Override
    public Map<String ,Stat> getCurrentServerStatMap(String taskType) throws Exception{
    	Map<String ,Stat> statMap=new HashMap<String ,Stat>();
    	String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType 
		        + "/" + taskType + "/" + this.PATH_Server;
//        /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/server

		List<String> childs=this.getZooKeeper().getChildren(zkPath, false);
//        /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/server/myTbScheduleJob1$192.168.61.230$7581B84B828A44A686CB3D5D95D46D96$0000000562
        for(String serv :childs){
			String singleServ = zkPath + "/" + serv;
			Stat servStat= this.getZooKeeper().exists(singleServ, false);
			statMap.put(serv,servStat);
		}
    	return statMap;
    }

//    获取任务下/server节点data，之前尝试分配的时候，会将它修改。
    public long getReloadTaskItemFlag(String taskType) throws Exception{
    	String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType 
		        + "/" + taskType + "/" + this.PATH_Server;
		Stat stat = new Stat();
        //会将节点下的stat深度赋值给传入的值
		this.getZooKeeper().getData(zkPath, false, stat);
    	return stat.getVersion();
    }
	/**
	 * 根据基础配置里面的任务项来创建各个域里面的任务项0.0
	 * @param baseTaskType
	 * @param ownSign
	 * @param baseTaskItems
	 * @throws Exception
	 */
	public void createScheduleTaskItem(String baseTaskType, String ownSign,String[] baseTaskItems) throws Exception {
		ScheduleTaskItem[] taskItems = new ScheduleTaskItem[baseTaskItems.length];
		Pattern p = Pattern.compile("\\s*:\\s*\\{");
//		如果Item符合{：{}}，则item为 ：前的部分，parameter为：后{}内部的东西
		for (int i=0;i<baseTaskItems.length;i++){
			taskItems[i] = new ScheduleTaskItem();
			taskItems[i].setBaseTaskType(baseTaskType);
			taskItems[i].setTaskType(ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign));
			taskItems[i].setOwnSign(ownSign);
//			此处未设置 desc
			Matcher matcher = p.matcher(baseTaskItems[i]);
			if(matcher.find()){
				taskItems[i].setTaskItem(baseTaskItems[i].substring(0,matcher.start()).trim());
				taskItems[i].setDealParameter(baseTaskItems[i].substring(matcher.end(),baseTaskItems[i].length()-1).trim());
			}else{
				taskItems[i].setTaskItem(baseTaskItems[i]);
			}
			taskItems[i].setSts(ScheduleTaskItem.TaskItemSts.ACTIVTE);
		}
		createScheduleTaskItem(taskItems);
	}	
	/**
	 * 创建任务项
	 * /{$rootPath}/baseTaskType/{$baseTaskType}/{$taskType}/item/ 节点下创建各个item节点
	 * 注意其中的 CurrentSever和RequestServer没有设值，create可以直接设值的，下次注意
	 * @param taskItems
	 * @throws Exception
	 */
	public void createScheduleTaskItem(ScheduleTaskItem[] taskItems) throws Exception {
		for (ScheduleTaskItem taskItem : taskItems){
		   String zkPath = this.PATH_BaseTaskType + "/" + taskItem.getBaseTaskType() + "/" + taskItem.getTaskType() +"/" + this.PATH_TaskItem;
		   if(this.getZooKeeper().exists(zkPath, false)== null){
			   ZKTools.createPath(this.getZooKeeper(), zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
		   }
		   String zkTaskItemPath = zkPath + "/" + taskItem.getTaskItem();
		   this.getZooKeeper().create(zkTaskItemPath,null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
		   this.getZooKeeper().create(zkTaskItemPath + "/cur_server",null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
		   this.getZooKeeper().create(zkTaskItemPath + "/req_server",null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
		   this.getZooKeeper().create(zkTaskItemPath + "/sts",taskItem.getSts().toString().getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
		   this.getZooKeeper().create(zkTaskItemPath + "/parameter",taskItem.getDealParameter().getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
//			此处设的什么值
		   this.getZooKeeper().create(zkTaskItemPath + "/deal_desc",taskItem.getDealDesc().getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
		}
	}
	
	public void updateScheduleTaskItemStatus(String taskType,String taskItem,ScheduleTaskItem.TaskItemSts sts,String message) throws Exception{
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType +"/" + this.PATH_TaskItem +"/" + taskItem;
		if(this.getZooKeeper().exists(zkPath +"/sts", false)== null){
			this.getZooKeeper().setData(zkPath +"/sts", sts.toString().getBytes(), -1);
		}
		if(this.getZooKeeper().exists(zkPath +"/deal_desc", false)== null){
			if(message == null){
				message = "";
			}
			this.getZooKeeper().setData(zkPath +"/deal_desc",message.getBytes(), -1);
		}
	}

	/**
	 * 删除任务项
	 * @param taskType
	 * @param taskItem
	 * @throws Exception 
	 */
	public void deleteScheduleTaskItem(String taskType,String taskItem) throws Exception{
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType +"/" + this.PATH_TaskItem +"/" + taskItem;
		ZKTools.deleteTree(this.getZooKeeper(), zkPath);
	}

    /**
     * 获取任务节点下的所有item的所有节点的data信息
     * @param taskType
     * @return
     * @throws Exception
     */
	public List<ScheduleTaskItem> loadAllTaskItem(String taskType) throws Exception {
		List<ScheduleTaskItem> result = new ArrayList<ScheduleTaskItem>();
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType+"/" + baseTaskType +"/" + taskType + "/" + this.PATH_TaskItem;
		if(this.getZooKeeper().exists(zkPath, false)==null){
		    return result;	
		}
		List<String> taskItems = this.getZooKeeper().getChildren(zkPath, false);
//		 Collections.sort(taskItems);
//		20150323 有些任务分片，业务方其实是用数字的字符串排序的。优先以数字进行排序，否则以字符串排序
		Collections.sort(taskItems,new Comparator<String>(){
			public int compare(String u1, String u2) {
				if(StringUtils.isNumeric(u1) && StringUtils.isNumeric(u2)){
					int iU1= Integer.parseInt(u1);
					int iU2= Integer.parseInt(u2);
					if(iU1==iU2){
						return 0 ;
					}else if(iU1>iU2){
						return 1 ;
					}else{
						return -1;
					}
				}else{
					return u1.compareTo(u2);
				}
			}
		});
		for(String taskItem:taskItems){
			ScheduleTaskItem info = new ScheduleTaskItem();
			info.setTaskType(taskType);
			info.setTaskItem(taskItem);
			String zkTaskItemPath = zkPath + "/" + taskItem;
			byte[] curContent = this.getZooKeeper().getData(zkTaskItemPath+"/cur_server",false,null);
			if(curContent != null){
			    info.setCurrentScheduleServer(new String(curContent));
			}
			byte[] reqContent = this.getZooKeeper().getData(zkTaskItemPath+"/req_server",false,null);
			if(reqContent != null){
			    info.setRequestScheduleServer(new String(reqContent));
			}
			byte[] stsContent = this.getZooKeeper().getData(zkTaskItemPath+"/sts",false,null);
			if(stsContent != null){
			    info.setSts(ScheduleTaskItem.TaskItemSts.valueOf(new String(stsContent)));
			}
			byte[] parameterContent = this.getZooKeeper().getData(zkTaskItemPath+"/parameter",false,null);
			if(parameterContent != null){
			    info.setDealParameter(new String(parameterContent));
			}
			byte[] dealDescContent = this.getZooKeeper().getData(zkTaskItemPath+"/deal_desc",false,null);
			if(dealDescContent != null){
			    info.setDealDesc(new String(dealDescContent));
			}
			result.add(info);
		}
		return result;
		
	}

	/**
	 * 获取任务信息
	 * @param baseTaskType
	 * @return
	 * @throws Exception
	 */
	public ScheduleTaskType loadTaskTypeBaseInfo(String baseTaskType)
			throws Exception {
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
		if(this.getZooKeeper().exists(zkPath, false) == null){
			return null;
		}
        /**
         * 获取的数据分为
         * ScheduleTaskType
         * {"baseTaskType":"myTbScheduleJob1","heartBeatRate":5000,"judgeDeadInterval":60000,"sleepTimeNoData":500,"sleepTimeInterval":0,"fetchDataNumber":500,"executeNumber":1,"threadNumber":4,"processorType":"SLEEP","permitRunStartTime":"0 * * * * ?","expireOwnSignInterval":1.0,"dealBeanName":"myTbScheduleJob","taskParameter":"","taskKind":"static","taskItems":["month\u003d12","key\u003d1","a\u003d3","b\u003d4","c\u003d5"],"maxTaskItemsOfOneThreadGroup":0,"version":0,"sts":"resume"}
         *
         * 和    stat
         cZxid = 0x3448
         ctime = Thu Oct 20 19:14:26 CST 2016
         mZxid = 0xa911
         mtime = Fri Oct 21 16:25:18 CST 2016
         pZxid = 0x11fd0
         cversion = 25
         dataVersion = 2
         aclVersion = 0
         ephemeralOwner = 0x0
         dataLength = 478
         numChildren = 1
         */
		String valueString= new String(this.getZooKeeper().getData(zkPath,false,null));

//      页面配置的任务信息
		ScheduleTaskType result = (ScheduleTaskType)this.gson.fromJson(valueString, ScheduleTaskType.class);
		return result;
	}

	@Override
	public List<ScheduleTaskType> getAllTaskTypeBaseInfo() throws Exception {
		String zkPath = this.PATH_BaseTaskType;
		List<ScheduleTaskType> result = new ArrayList<ScheduleTaskType>();
		List<String> names = this.getZooKeeper().getChildren(zkPath,false);
		Collections.sort(names);
		for(String name:names){
			result.add(this.loadTaskTypeBaseInfo(name));
		}
		return result;
	}

	/**
	 * 页面调用的清理的o.o
	 * 运行期信息清理成功
	 * @param baseTaskType
	 * @throws Exception
	 */
	@Override
	public void clearTaskType(String baseTaskType) throws Exception {
		//清除所有的Runtime TaskType		
		String zkPath =this.PATH_BaseTaskType+"/" + baseTaskType; 
		List<String> list = this.getZooKeeper().getChildren(zkPath,false);
		for (String name : list) {
			ZKTools.deleteTree(this.getZooKeeper(),zkPath + "/" + name);
		}
	}

	@Override
	public List<ScheduleTaskTypeRunningInfo> getAllTaskTypeRunningInfo(
			String baseTaskType) throws Exception {
		List<ScheduleTaskTypeRunningInfo> result = new ArrayList<ScheduleTaskTypeRunningInfo>();
		String zkPath =this.PATH_BaseTaskType+"/" + baseTaskType; 
		if(this.getZooKeeper().exists(zkPath, false)==null){
			return result;
		}
		List<String> list = this.getZooKeeper().getChildren(zkPath,false);
		Collections.sort(list);
		
		for(String name:list){
			ScheduleTaskTypeRunningInfo info = new ScheduleTaskTypeRunningInfo();
			info.setBaseTaskType(baseTaskType);
			info.setTaskType(name);
			info.setOwnSign(ScheduleUtil.splitOwnsignFromTaskType(name));
			result.add(info);
		}
		return result;
	}

	/**
	 * 页面删除任务
	 * @param baseTaskType
	 * @throws Exception
	 */
	@Override
	public void deleteTaskType(String baseTaskType) throws Exception {
		ZKTools.deleteTree(this.getZooKeeper(),this.PATH_BaseTaskType + "/" + baseTaskType);
	}

	@Override
	public List<ScheduleServer> selectScheduleServer(String baseTaskType,
			String ownSign, String ip, String orderStr) throws Exception {
		List<String> names = new ArrayList<String>();
		if(baseTaskType != null && ownSign != null){
			names.add(baseTaskType +"$"+ ownSign) ;
		}else if(baseTaskType != null && ownSign == null){
			if(this.getZooKeeper().exists(this.PATH_BaseTaskType+"/" + baseTaskType,false) != null){
				for(String name:this.getZooKeeper().getChildren(this.PATH_BaseTaskType+"/" + baseTaskType,false)){
					names.add(name);
				}
			}
		}else if(baseTaskType == null){
			for(String name:this.getZooKeeper().getChildren(this.PATH_BaseTaskType,false)){
				if(ownSign != null){
					names.add(name + "$" + ownSign);
				}else{
					for(String str:this.getZooKeeper().getChildren(this.PATH_BaseTaskType+"/" + name,false)){
						names.add(str);
					}
				}
			}
		}
		List<ScheduleServer> result= new ArrayList<ScheduleServer>();
		for(String name:names){
			List<ScheduleServer> tempList = this.selectAllValidScheduleServer(name);
			if(ip == null){
				result.addAll(tempList);
			}else{
				for(ScheduleServer server:tempList){
					if(ip.equals(server.getIp())){
						result.add(server);
					}
				}
			}
		}
		Collections.sort(result,new ScheduleServerComparator(orderStr));
		//排序
		return result;
	}

	@Override
	public List<ScheduleServer> selectHistoryScheduleServer(
			String baseTaskType, String ownSign, String ip, String orderStr)
			throws Exception {
		throw new Exception("没有实现的方法");
	}

    /**
     *  获取给定的uuid即server 分配到的任务项
     *  条件是任务/taskItem/具体的taskItem/cur_server 的 data 就是给定的server，
	 *  返回的是taskItem
     * @param taskType
     *            任务类型
     * @param uuid
     *            当前server的UUID
     * @return
     * @throws Exception
     */
	@Override
	public List<TaskItemDefine> reloadDealTaskItem(String taskType, String uuid)
			throws Exception {
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
		 
		 List<String> taskItems = this.getZooKeeper().getChildren(zkPath, false);
//		 Collections.sort(taskItems);
//		 有些任务分片，业务方其实是用数字的字符串排序的。优先以字符串方式进行排序
		Collections.sort(taskItems,new Comparator<String>(){
			public int compare(String u1, String u2) {
				if(StringUtils.isNumeric(u1) && StringUtils.isNumeric(u2)){
					int iU1= Integer.parseInt(u1);
					int iU2= Integer.parseInt(u2);
					if(iU1==iU2){
						return 0 ;
					}else if(iU1>iU2){
						return 1 ;
					}else{
						return -1;
					}
				}else{
					return u1.compareTo(u2);
				}
			}
		});
		 
		log.debug(taskType+ " current uid="+uuid+" , zk  reloadDealTaskItem");
		
		 List<TaskItemDefine> result = new ArrayList<TaskItemDefine>();
		 for(String name:taskItems){
			byte[] value = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server",false,null);
//             如果server被设置成了前服务器
			if(value != null && uuid.equals(new String(value))){
				TaskItemDefine item = new TaskItemDefine();//在此创建taskItemDefine
				item.setTaskItemId(name);//将任务项的赋值给Id 逗号分隔的 任务项目
				byte[] parameterValue = this.getZooKeeper().getData(zkPath + "/" + name + "/parameter",false,null);
				if(parameterValue != null){
					item.setParameter(new String(parameterValue));
				}
				result.add(item);
				
				 
				 
			}else if(value != null && uuid.equals(new String(value))==false){
				log.trace(" current uid="+uuid+" , zk cur_server uid="+new String(value));
			}else  {
				log.trace(" current uid="+uuid);
			}
		 }
		 return result;
	}

    /**
     * 遍历任务的任务项节点，若cur_server节点值是当前server，但是req不为空，则需要设置当前server为要求的server，且设置modif为true，即设置任务的server都需要重载

     * @param taskType
     * @param uuid
     * @throws Exception
     */
	@Override
	public void releaseDealTaskItem(String taskType, String uuid) throws Exception {
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
		 boolean isModify = false;
		 for(String name:this.getZooKeeper().getChildren(zkPath, false)){
			byte[] curServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server",false,null);
			byte[] reqServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/req_server",false,null);
			if(reqServerValue != null && curServerValue != null && uuid.equals(new String(curServerValue))==true){
				this.getZooKeeper().setData(zkPath + "/" + name + "/cur_server",reqServerValue,-1);
				this.getZooKeeper().setData(zkPath + "/" + name + "/req_server",null,-1);		
				isModify = true;
			}
		 }
		 if(isModify == true){ //设置需要所有的服务器重新装载任务
			 this.updateReloadTaskItemFlag(taskType);
		 }
	}

	@Override
	public int queryTaskItemCount(String taskType) throws Exception {
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
		 return this.getZooKeeper().getChildren(zkPath, false).size();
	}

	/**
	 * 删除已经超过一天没更新的 任务
	 * 即任务节点下的taskItem的修改时间是否>一天没有修改了
	 * @param baseTaskType
//	 * @param serverUUID 服务器 没什么用
	 * @param expireDateInternal 过期时间，以天为单位
	 * @throws Exception
	 */
	public void clearExpireTaskTypeRunningInfo(String baseTaskType,String serverUUID, double expireDateInternal) throws Exception {
//		/{$rootPath}/baseTaskType/{$taskName} 下边不是只有{$taskName}吗？
		for (String name : this.getZooKeeper().getChildren(this.PATH_BaseTaskType + "/" + baseTaskType, false)) {
//			/tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/ + taskItem
			String zkPath = this.PATH_BaseTaskType+"/"+ baseTaskType +"/" + name +"/" + this.PATH_TaskItem;
			Stat stat = this.getZooKeeper().exists(zkPath, false);// 存在节点就不为空，stat就是在shell中get时出来的参数
			//Stat 中的mTime 是最近一次的修改时间戳
			//
			if (stat == null || getSystemTime() - stat.getMtime() > (long) (expireDateInternal * 24 * 3600 * 1000)) {
				ZKTools.deleteTree(this.getZooKeeper(),this.PATH_BaseTaskType +"/" + baseTaskType + "/" + name);
			}
		}
	}

	/**
	 * 消除任务分配的过期的 server
	 * @param taskType
	 * @param expireTime
	 * @return
	 * @throws Exception
	 */
	@Override
	public int clearExpireScheduleServer(String taskType,long expireTime) throws Exception {
		 int result =0;
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType 
		        + "/" + taskType + "/" + this.PATH_Server;
		 if(this.getZooKeeper().exists(zkPath,false)== null){
			 String tempPath =this.PATH_BaseTaskType + "/" + baseTaskType   + "/" + taskType;
			 if(this.getZooKeeper().exists(tempPath,false)== null){
				 this.getZooKeeper().create(tempPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
			 }
			 this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
		 }
		for (String name : this.getZooKeeper().getChildren(zkPath, false)) {
			try {
				Stat stat = this.getZooKeeper().exists(zkPath + "/" + name,false);
				if (getSystemTime() - stat.getMtime() > expireTime) {
					ZKTools.deleteTree(this.getZooKeeper(), zkPath + "/" + name);
					result++;
				}
			} catch (Exception e) {
				// 当有多台服务器时，存在并发清理的可能，忽略异常，
				// 怎么会有多个清理呢？0.0
				result++;
			}
		}
		return result;
	}

    /**
     * 遍历任务的任务项的cur_server节点，
     * 若节点不存在 结果+1
     * 若存在服务器，且服务器不在给定的server列表中，则设置data为空即任务不能再分配给不存在的服务器。 结果+1
     *
     * 返回的是没有分配服务器的taskItem数量
     *
     * @param taskType
     * @param serverList
     * @return
     * @throws Exception
     */
	@Override
	public int clearTaskItem(String taskType,
			List<String> serverList) throws Exception {
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
//       /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/taskItem

		 int result =0;
		 for(String name:this.getZooKeeper().getChildren(zkPath, false)){
			byte[] curServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server",false,null);
//             /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/taskItem/key=1/cur_server 其中key=1 是taskItem中的一个

			if(curServerValue != null){
			   String curServer = new String(curServerValue);
			   boolean isFind = false;
			   for(String server : serverList){
				   if(curServer.equals(server)){
					   isFind = true;
				       break;
				   }
			   }
			   if(isFind == false){
				   this.getZooKeeper().setData(zkPath + "/" + name + "/cur_server",null,-1);	
				   result = result + 1;
			   }
			}else{
				result = result + 1;
			}
		 }
		 return result;
	}

    /**
     * 查询任务下分配的server列表
     *
     *
     * baseTaskType 与 taskType的区别就是 taskType 可能会有"$sth"
     * @param taskType
     * @return
     * @throws Exception
     */
	public List<String> loadScheduleServerNames(String taskType)throws Exception{
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
		 if(this.getZooKeeper().exists(zkPath, false)== null){
			 return new ArrayList<String>();
		 }
		 List<String> serverList = this.getZooKeeper().getChildren(zkPath, false);
		 Collections.sort(serverList,new Comparator<String>(){
				public int compare(String u1, String u2) {
					return u1.substring(u1.lastIndexOf("$") + 1).compareTo(
							u2.substring(u2.lastIndexOf("$") + 1));
				}
			});
		 return serverList;
	}

	@Override
	public List<ScheduleServer> selectAllValidScheduleServer(String taskType)
			throws Exception {
		List<ScheduleServer> result = new ArrayList<ScheduleServer>();
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
		 if(this.getZooKeeper().exists(zkPath, false)== null){
			 return result;
		 }
		 List<String> serverList = this.getZooKeeper().getChildren(zkPath, false);
		 Collections.sort(serverList,new Comparator<String>(){
				public int compare(String u1, String u2) {
					return u1.substring(u1.lastIndexOf("$") + 1).compareTo(
							u2.substring(u2.lastIndexOf("$") + 1));
				}
			});
		for(String name:serverList){
			try{
			String valueString= new String(this.getZooKeeper().getData(zkPath + "/"+ name,false,null));
			ScheduleServer server = (ScheduleServer)this.gson.fromJson(valueString,ScheduleServer.class);
			server.setCenterServerTime(new Timestamp(this.getSystemTime()));
			result.add(server);
			}catch(Exception e){
				log.debug(e.getMessage(), e);
			}
		}
		return result;
	}
	
	public List<ScheduleServer> selectScheduleServerByManagerFactoryUUID(String factoryUUID)
			throws Exception {
		 List<ScheduleServer> result = new ArrayList<ScheduleServer>();
		 for(String baseTaskType: this.getZooKeeper().getChildren(this.PATH_BaseTaskType,false)){
			for(String taskType: this.getZooKeeper().getChildren(this.PATH_BaseTaskType+"/"+baseTaskType,false)){
				String zkPath =  this.PATH_BaseTaskType+"/"+baseTaskType+"/"+taskType+"/"+this.PATH_Server;
				for(String uuid: this.getZooKeeper().getChildren(zkPath,false)){
					String valueString= new String(this.getZooKeeper().getData(zkPath + "/"+ uuid,false,null));
					ScheduleServer server = (ScheduleServer)this.gson.fromJson(valueString,ScheduleServer.class);
					server.setCenterServerTime(new Timestamp(this.getSystemTime()));
					if (server.getManagerFactoryUUID().equals(factoryUUID)) {
						result.add(server);
					}
				}		  
			}
		 }
		 Collections.sort(result,new Comparator<ScheduleServer>(){
				public int compare(ScheduleServer u1, ScheduleServer u2) {
					int result = u1.getTaskType().compareTo(u2.getTaskType());
					if(result == 0){
						String s1 = u1.getUuid();
						String s2 = u2.getUuid();
						result = s1.substring(s1.lastIndexOf("$") + 1).compareTo(
								s2.substring(s2.lastIndexOf("$") + 1));
					}
					return result;
				}
			});
		return result;
	}

    /**
     * 遍历所有server，其中序号最小的为leader
     * @param serverList
     * @return
     */
	public String getLeader(List<String> serverList){
		if(serverList == null || serverList.size() ==0){
			return "";
		}
		long no = Long.MAX_VALUE;
		long tmpNo = -1;
		String leader = null;
    	for(String server:serverList){
    		tmpNo =Long.parseLong( server.substring(server.lastIndexOf("$")+1));
    		if(no > tmpNo){
    			no = tmpNo;
    			leader = server;
    		}
    	}
    	return leader;
    }
    public boolean isLeader(String uuid,List<String> serverList){
    	return uuid.equals(getLeader(serverList));
    }

    /**
     * 分配任务项
     * 需要分配，只是修改req_server 下的数据
     * 若都不需要修改，则不做操作
     * @param taskType
     * @param currentUuid
     * @param maxNumOfOneServer
     * @param taskServerList
     * @throws Exception
     */
	@Override
	public void assignTaskItem(String taskType, String currentUuid,int maxNumOfOneServer,
			List<String> taskServerList) throws Exception {
		 if(this.isLeader(currentUuid,taskServerList)==false){
			 if(log.isDebugEnabled()){
			   log.debug(currentUuid +":不是负责任务分配的Leader,直接返回");
			 }
			 return;
		 }
		 if(log.isDebugEnabled()){
			   log.debug(currentUuid +":开始重新分配任务......");
		 }		
		 if(taskServerList.size()<=0){
			 //在服务器动态调整的时候，可能出现服务器列表为空的清空
			 return;
		 }
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
//        /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/taskItem

        List<String> children = this.getZooKeeper().getChildren(zkPath, false);
//        任务项列表
//        [key=1, b=4, c=5, month=12, a=3]
//		 Collections.sort(children);
//	     20150323 有些任务分片，业务方其实是用数字的字符串排序的。优先以数字进行排序，否则以字符串排序
		 Collections.sort(children,new Comparator<String>(){
			 public int compare(String u1, String u2) {
					if(StringUtils.isNumeric(u1) && StringUtils.isNumeric(u2)){
						int iU1= Integer.parseInt(u1);
						int iU2= Integer.parseInt(u2);
						if(iU1==iU2){
							return 0 ;
						}else if(iU1>iU2){
							return 1 ;
						}else{
							return -1;
						}
					}else{
						return u1.compareTo(u2);
					}
				}
			});
		 int unModifyCount =0;
		 int[] taskNums = ScheduleUtil.assignThreadGroupNum(taskServerList.size(), children.size(), maxNumOfOneServer);
		 int point =0;//server序号
		 int count = 0;
		 String NO_SERVER_DEAL = "没有分配到服务器";
        /**
         * 算法说明
         * taskNum 为{3,3,2}
         * server 为 3
         * children 为 8
         *
         *
         *
         */
		 for(int i=0;i <children.size();i++){
			String name = children.get(i);
             /**
              * count       0       3       6
              * point       0       1       2
              * taskItem  0,1,2   3,4,5    6,7
              * 所以并不是像ppt说的那种分，但，是取模
              */

             if(point <taskServerList.size() && i >= count + taskNums[point]){
				count = count + taskNums[point];
				point = point + 1;
			}


			String serverName = NO_SERVER_DEAL;
			if(point < taskServerList.size() ){
				serverName = taskServerList.get(point);
			}


			byte[] curServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server",false,null);
			byte[] reqServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/req_server",false,null);

             /**
              * 1，如果当前没有分配服务，则直接设置为当前服务。
              * 2，如果分配服务就是当前服务，且要求的是当前服务，则什么都不做
              * 3，将要求的服务设置为当期服务
              */
             if(curServerValue == null || new String(curServerValue).equals(NO_SERVER_DEAL)){
				this.getZooKeeper().setData(zkPath + "/" + name + "/cur_server",serverName.getBytes(),-1);
				this.getZooKeeper().setData(zkPath + "/" + name + "/req_server",null,-1);
			}else if(new String(curServerValue).equals(serverName)==true && reqServerValue == null ){
				//不需要做任何事情
				unModifyCount = unModifyCount + 1;
			}else{
				this.getZooKeeper().setData(zkPath + "/" + name + "/req_server",serverName.getBytes(),-1);
			}
		 }	

        //只要需要重新操作的数量不等于taskItem的数量，就要设置为 reldad=true，这个时候设置的会把stat的verison修改
//        /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/server
		 if(unModifyCount < children.size()){ //设置需要所有的服务器重新装载任务
			 log.info("设置需要所有的服务器重新装载任务:updateReloadTaskItemFlag......"+taskType+ "  ,currentUuid "+currentUuid );

			 this.updateReloadTaskItemFlag(taskType);
		 }
		 if(log.isDebugEnabled()){
			 StringBuffer buffer = new StringBuffer();
			 for(ScheduleTaskItem taskItem: this.loadAllTaskItem(taskType)){
				buffer.append("\n").append(taskItem.toString());
			 }
			 log.debug(buffer.toString());
		 }
	}
	public void assignTaskItem22(String taskType, String currentUuid,
			List<String> serverList) throws Exception {
		 if(this.isLeader(currentUuid,serverList)==false){
			 if(log.isDebugEnabled()){
			   log.debug(currentUuid +":不是负责任务分配的Leader,直接返回");
			 }
			 return;
		 }
		 if(log.isDebugEnabled()){
			   log.debug(currentUuid +":开始重新分配任务......");
		 }		
		 if(serverList.size()<=0){
			 //在服务器动态调整的时候，可能出现服务器列表为空的清空
			 return;
		 }
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
		 int point =0;
		 List<String> children = this.getZooKeeper().getChildren(zkPath, false);
//		 Collections.sort(children);
//		 20150323 有些任务分片，业务方其实是用数字的字符串排序的。优先以数字进行排序，否则以字符串排序
		 Collections.sort(children,new Comparator<String>(){
			 public int compare(String u1, String u2) {
					if(StringUtils.isNumeric(u1) && StringUtils.isNumeric(u2)){
						int iU1= Integer.parseInt(u1);
						int iU2= Integer.parseInt(u2);
						if(iU1==iU2){
							return 0 ;
						}else if(iU1>iU2){
							return 1 ;
						}else{
							return -1;
						}
					}else{
						return u1.compareTo(u2);
					}
				}
			});
		 int unModifyCount =0;
		 for(String name:children){
			byte[] curServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server",false,null);
			byte[] reqServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/req_server",false,null);
			if(curServerValue == null){
				this.getZooKeeper().setData(zkPath + "/" + name + "/cur_server",serverList.get(point).getBytes(),-1);
				this.getZooKeeper().setData(zkPath + "/" + name + "/req_server",null,-1);
			}else if(new String(curServerValue).equals(serverList.get(point))==true && reqServerValue == null ){
				//不需要做任何事情
				unModifyCount = unModifyCount + 1;
			}else{
				this.getZooKeeper().setData(zkPath + "/" + name + "/req_server",serverList.get(point).getBytes(),-1);
			}
			point = (point  + 1) % serverList.size();
		 }	
		 
		 if(unModifyCount < children.size()){ //设置需要所有的服务器重新装载任务
			 this.updateReloadTaskItemFlag(taskType);
		 }
		 if(log.isDebugEnabled()){
			 StringBuffer buffer = new StringBuffer();
			 for(ScheduleTaskItem taskItem: this.loadAllTaskItem(taskType)){
				buffer.append("\n").append(taskItem.toString());
			 }
			 log.debug(buffer.toString());
		 }
	}


    /**
     * 注册一个scheduleServer
     * 在zookeeper中的baseTaskType路径下的任务名称/策略任务名称/server下创建该server的节点，并设置server对象为data
     * @param server
     * @throws Exception
     */
	public void registerScheduleServer(ScheduleServer server) throws Exception {
		if(server.isRegister() == true){
			throw new Exception(server.getUuid() + " 被重复注册");
		}
//		/tbSchedule/haha/baseTaskType/任务名称不含（不含ownSign)/页面策略配置时的任务名称
        String zkPath = this.PATH_BaseTaskType + "/" + server.getBaseTaskType() +"/" + server.getTaskType();
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
		}
//		/tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1/server
		zkPath = zkPath +"/" + this.PATH_Server; //的确是没有设值
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
		}
		String realPath = null;
		//此处必须增加UUID作为唯一性保障
		String zkServerPath = zkPath + "/" + server.getTaskType() + "$"+ server.getIp() + "$"
				+ (UUID.randomUUID().toString().replaceAll("-", "").toUpperCase())+"$";
		realPath = this.getZooKeeper().create(zkServerPath, null, this.zkManager.getAcl(),CreateMode.PERSISTENT_SEQUENTIAL);
//      /tbSchedule/haha/baseTaskType/myTbScheduleJob1/myTbScheduleJob1（$）/server/
//         myTbScheduleJob1$192.168.61.230$F96B8C0FA4174D63B75062309A2CD21B$0000000100
		server.setUuid(realPath.substring(realPath.lastIndexOf("/") + 1));
		
		Timestamp heartBeatTime = new Timestamp(this.getSystemTime());
		server.setHeartBeatTime(heartBeatTime);

        /**
         * value，会设置成该路径的data
         * {"uuid":"myTbScheduleJob1$192.168.61.230$F96B8C0FA4174D63B75062309A2CD21B$0000000100","id":16102638086727,"taskType":"myTbScheduleJob1","baseTaskType":"myTbScheduleJob1","ownSign":"BASE","ip":"192.168.61.230","hostName":"liuguobin.local","threadNum":4,"registerTime":"2016-10-26 19:10:48","heartBeatTime":"2016-10-27 17:06:14","lastFetchDataTime":"2016-10-26 23:15:52","dealInfoDesc":"没有数据,暂停调度:FetchDataCount\u003d126,FetchDataNum\u003d0,DealDataSucess\u003d0,DealDataFail\u003d0,DealSpendTime\u003d0,otherCompareCount\u003d0","nextRunStartTime":"2016-10-26 21:16:00","nextRunEndTime":"当不能获取到数据的时候pause","version":6340,"isRegister":true,"managerFactoryUUID":"192.168.61.230$liuguobin.local$5D6A9687D8E74F7DAC784760131986AF$0000000066"}
         cZxid = 0x259b6
         ctime = Thu Oct 27 13:36:50 CST 2016
         mZxid = 0x2e9ab
         mtime = Thu Oct 27 17:06:14 CST 2016
         pZxid = 0x259b6
         cversion = 0
         dataVersion = 2512
         aclVersion = 0
         ephemeralOwner = 0x0
         dataLength = 774
         numChildren = 0
         */
        String valueString = this.gson.toJson(server);
		this.getZooKeeper().setData(realPath,valueString.getBytes(),-1);
		server.setRegister(true);
	}

    /**
     * 刷新server节点数据
	 * 若节点不存在，设置未注册，返回false；
	 * 节点存在，则修改heartBeatTime和版本 +1
     *
     * @param server
     * @return
     * @throws Exception
     */
	public boolean refreshScheduleServer(ScheduleServer server) throws Exception {
		Timestamp heartBeatTime = new Timestamp(this.getSystemTime());
    	String zkPath = this.PATH_BaseTaskType + "/" + server.getBaseTaskType() + "/" + server.getTaskType() 
    	    + "/" + this.PATH_Server +"/" + server.getUuid();
//        节点不存在，设置为未注册 返回false
//        存在，修改heartBeatTime，版本+1 返回true
    	if(this.getZooKeeper().exists(zkPath, false)== null){
    		//数据可能被清除，先清除内存数据后，重新注册数据
    		server.setRegister(false);
    		return false;
    	}else{
    		Timestamp oldHeartBeatTime = server.getHeartBeatTime();
    		server.setHeartBeatTime(heartBeatTime);
    		server.setVersion(server.getVersion() + 1);//会不一致吗？好像之前也没有设置data 0.0
    		String valueString = this.gson.toJson(server);
    		try{
    			this.getZooKeeper().setData(zkPath,valueString.getBytes(),-1);
    		}catch(Exception e){
    			//恢复上次的心跳时间
    			server.setHeartBeatTime(oldHeartBeatTime);
    			server.setVersion(server.getVersion() - 1);
    			throw e;
    		}
    		return true;
    	}
	}

	/**
	 * 注销掉调度服务器，
	 * 即删除task的server下的该节点
	 * @param taskType
	 * @param serverUUID
	 * @throws Exception
	 */
	public void unRegisterScheduleServer(String taskType,String serverUUID) throws Exception {
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server  + "/"+ serverUUID;
		 if(this.getZooKeeper().exists(zkPath, false) != null){
	    	this.getZooKeeper().delete(zkPath, -1);
		 }
	}

	/**
	 * 暂停任务的执行 0.0
	 * @param baseTaskType
	 * @throws Exception
	 */
	@Override
	public void pauseAllServer(String baseTaskType) throws Exception {
		ScheduleTaskType taskType = this.loadTaskTypeBaseInfo(baseTaskType);
		taskType.setSts(ScheduleTaskType.STS_PAUSE);
		this.updateBaseTaskType(taskType);
	}

	/**
	 * 恢复任务的执行 0.0
	 * @param baseTaskType
	 * @throws Exception
	 */
	@Override
	public void resumeAllServer(String baseTaskType) throws Exception {
		ScheduleTaskType taskType = this.loadTaskTypeBaseInfo(baseTaskType);
		taskType.setSts(ScheduleTaskType.STS_RESUME);
		this.updateBaseTaskType(taskType);
	}

	
	public long getSystemTime(){
		return this.zkBaseTime + ( System.currentTimeMillis() - this.loclaBaseTime);
	}

}

class ScheduleServerComparator implements Comparator<ScheduleServer>{
	String[] orderFields;
	public ScheduleServerComparator(String aOrderStr){
		if(aOrderStr != null){
			orderFields = aOrderStr.toUpperCase().split(",");
		}else{
			orderFields = "TASK_TYPE,OWN_SIGN,REGISTER_TIME,HEARTBEAT_TIME,IP".toUpperCase().split(",");
		}
	}
	public int compareObject(String o1,String o2){
		if(o1==null && o2 == null){
			return 0;
		}else if(o1 != null){
			return o1.compareTo(o2);
		}else {
			return -1;
		}
	}
	public int compareObject(Timestamp o1,Timestamp o2){
		if(o1==null && o2 == null){
			return 0;
		}else if(o1 != null){
			return o1.compareTo(o2);
		}else {
			return -1;
		}
	}
	public int compare(ScheduleServer o1, ScheduleServer o2) {
		int result = 0;
		for(String name : orderFields){
			if(name.equals("TASK_TYPE")){
				result =compareObject( o1.getTaskType(),o2.getTaskType());
				if(result != 0){
				   return result;
				}
			}else if(name.equals("OWN_SIGN")){
				result = compareObject(o1.getOwnSign(),o2.getOwnSign());
				if(result != 0){
				   return result;
				}
			}else if(name.equals("REGISTER_TIME")){
				result = compareObject(o1.getRegisterTime(),o2.getRegisterTime());
				if(result != 0){
					return result;
				}
			}else if(name.equals("HEARTBEAT_TIME")){
				result =compareObject( o1.getHeartBeatTime(),o2.getHeartBeatTime());
				if(result != 0){
					return result;
				}
			}else if(name.equals("IP")){
				result = compareObject(o1.getIp(),o2.getIp());
				if(result != 0){
					return result;
				}
			}else if(name.equals("MANAGER_FACTORY")){
				result = compareObject(o1.getManagerFactoryUUID(),o2.getManagerFactoryUUID());
				if(result != 0){
					return result;
				}
			}
		}
		return result;
	}
}
class TimestampTypeAdapter implements JsonSerializer<Timestamp>, JsonDeserializer<Timestamp>{   
    public JsonElement serialize(Timestamp src, Type arg1, JsonSerializationContext arg2) {   
    	DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
        String dateFormatAsString = format.format(new Date(src.getTime()));   
        return new JsonPrimitive(dateFormatAsString);   
    }   
  
    public Timestamp deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {   
        if (!(json instanceof JsonPrimitive)) {   
            throw new JsonParseException("The date should be a string value");   
        }   
  
        try {   
        	DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
            Date date = (Date) format.parse(json.getAsString());   
            return new Timestamp(date.getTime());   
        } catch (Exception e) {   
            throw new JsonParseException(e);   
        }   
    }   

}  



 