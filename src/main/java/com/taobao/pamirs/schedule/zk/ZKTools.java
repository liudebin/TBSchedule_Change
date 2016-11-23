package com.taobao.pamirs.schedule.zk;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Zookeeper的使用
 * 创建路径，打印树，删除树等
 */
public class ZKTools {
	public static void createPath(ZooKeeper zk, String path,CreateMode createMode, List<ACL> acl) throws Exception {
		String[] list = path.split("/");
		String zkPath = "";
		for (String str : list) {
			if (!str.equals("")) {
				zkPath = zkPath + "/" + str;
				if (zk.exists(zkPath, false) == null) {
					/**
					 * 参数一 节点路径地址
					 * 参数二 想要保存的数据，需要转换成字节数组
					 * 参数三：ACL访问控制列表（Access control list）,
					 *      参数类型为ArrayList<ACL>，Ids接口提供了一些默认的值可以调用。
					 *      OPEN_ACL_UNSAFE     This is a completely open ACL
					 *                          这是一个完全开放的ACL，不安全
					 *      CREATOR_ALL_ACL     This ACL gives the
					 *                           creators authentication id's all permissions.
					 *                          这个ACL赋予那些授权了的用户具备权限
					 *      READ_ACL_UNSAFE     This ACL gives the world the ability to read.
					 *                          这个ACL赋予用户读的权限，也就是获取数据之类的权限。
					 *
					 *  参数四：创建的节点类型。枚举值CreateMode
					 *      PERSISTENT (0, false, false)
					 *      PERSISTENT_SEQUENTIAL (2, false, true)
					 *          这两个类型创建的都是持久型类型节点，回话结束之后不会自动删除。
					 *          区别在于，第二个类型所创建的节点名后会有一个单调递增的数值。
					 *          永久性，即客户端shutdown了也不会消失
					 *      EPHEMERAL (1, true, false)
					 *      EPHEMERAL_SEQUENTIAL (3, true, true)
					 *          这两个类型所创建的是临时型类型节点，在回话结束之后，自动删除。
					 *          区别在于，第二个类型所创建的临时型节点名后面会有一个单调递增的数值。
					 * 最后create()方法的返回值是创建的节点的实际路径
					 */
					zk.create(zkPath, null, acl, createMode);
				}
			}
		}
	}

   public static void printTree(ZooKeeper zk,String path,Writer writer,String lineSplitChar) throws Exception{
	   String[] list = getTree(zk,path);
	   Stat stat = new Stat();
	   for(String name:list){
		   byte[] value = zk.getData(name, false, stat);
		   if(value == null){
			   writer.write(name + lineSplitChar);
		   }else{
			   writer.write(name+"[v."+ stat.getVersion() +"]"+"["+ new String(value) +"]"  + lineSplitChar);
		   }
	   }
   }  
   public static void deleteTree(ZooKeeper zk,String path) throws Exception{
	   String[] list = getTree(zk,path);
	   for(int i= list.length -1;i>=0; i--){
		   zk.delete(list[i],-1);
	   }
   }
   
   public static String[] getTree(ZooKeeper zk,String path) throws Exception{
	   if(zk.exists(path, false) == null){
		   return new String[0];
	   }
	   List<String> dealList = new ArrayList<String>();
	   dealList.add(path);
	   int index =0;
	   while(index < dealList.size()){
		   String tempPath = dealList.get(index);
		   List<String> children = zk.getChildren(tempPath, false);
		   if(tempPath.equalsIgnoreCase("/") == false){
			   tempPath = tempPath +"/";
		   }
		   Collections.sort(children);
		   for(int i = children.size() -1;i>=0;i--){
			   dealList.add(index+1, tempPath + children.get(i));
		   }
		   index++;
	   }
	   return (String[])dealList.toArray(new String[0]);
   }
}
