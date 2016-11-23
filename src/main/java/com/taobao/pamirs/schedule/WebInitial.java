package com.taobao.pamirs.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;


public class WebInitial extends HttpServlet {
	private static final long serialVersionUID = 1L;
	Logger logger = LoggerFactory.getLogger(getClass());

	public void init() throws ServletException {
		super.init();
		try {
			logger.info("开始了？？？？？");
			ConsoleManager.initial();
		} catch (Exception e) {
			throw new ServletException(e);
		}
	}
}
