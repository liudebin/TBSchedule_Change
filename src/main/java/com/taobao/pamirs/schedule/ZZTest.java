package com.taobao.pamirs.schedule;

/**
 * 测试ConsoleManager，
 * Created by liuguobin on 2016/11/14.
 */
public class ZZTest {
    public static void main(String[] args) {
        try {
//            是不行的，必须要有整合Spring framework
            ConsoleManager.initial();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

