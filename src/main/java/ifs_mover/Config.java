/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* ifsmover is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version 
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* ifsmover 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* ifsmover 개발팀은 사전 공지, 허락, 동의 없이 ifsmover 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/

package ifs_mover;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	private static final Logger logger = LoggerFactory.getLogger(Config.class);
	private Properties properties;

	private URL url;
	private String mountPoint;
	private String endPoint;
	private String region;
	private String accessKey;
	private String secretKey;
	private String bucket;
	private String prefix;
	private String moveSize;
	private String type;

	private boolean isAWS;

	private final String MOUNT_POINT = "mountpoint";
	private final String END_POINT = "endpoint";
	private final String REGION = "region";
	private final String ACCESS_KEY = "access";
	private final String SECRET_KEY = "secret";
	private final String BUCKET = "bucket";
	private final String PREFIX = "prefix";
	private final String MOVE_SIZE = "move_size";

	private final String PROTOCOL = "http";

	public Config() {

	}

	public Config(String path) {
		properties = new Properties();
		try (InputStream myis = new FileInputStream(path)) {
			properties.load(myis);
		} catch (FileNotFoundException e) {
			logger.error("File not found, path : {}", path);
			System.exit(-1);
		} catch (IOException e) {
			logger.error("IOExecption : {}, path : {}", e.getMessage(), path);
			System.exit(-1);
		}
	}

	public void configure() {
		mountPoint = properties.getProperty(MOUNT_POINT);
		endPoint = properties.getProperty(END_POINT);
		accessKey = properties.getProperty(ACCESS_KEY);
		secretKey = properties.getProperty(SECRET_KEY);
		bucket = properties.getProperty(BUCKET);
		prefix = properties.getProperty(PREFIX);
		region = properties.getProperty(REGION);
		moveSize = properties.getProperty(MOVE_SIZE);

		if (mountPoint != null && !mountPoint.isEmpty() && !mountPoint.endsWith("/")) {
			mountPoint += "/";
		}

		if (prefix != null && !prefix.isEmpty()) {
			if (prefix.startsWith("/")) {
				prefix = prefix.substring(1);
				if (prefix.startsWith("/")) {
					prefix = prefix.substring(1);
				}
			}

			if (prefix.endsWith("/")) {
				prefix = prefix.substring(0, prefix.length() - 1);
			}
		}

		if (endPoint != null && !endPoint.isEmpty()) {
			endPoint = endPoint.toLowerCase();
			if (endPoint.startsWith(PROTOCOL)) {
				isAWS = false;
				try {
					url = new URL(endPoint);
				} catch (MalformedURLException e) {
					logger.error(e.getMessage());
				}
			} else {
				isAWS = true;
			}
		} else {
			isAWS = false;
		}
	}

	public boolean isAWS() {
		return isAWS;
	}

	public String getMountPoint() {
		return mountPoint;
	}

	public String getEndPointProtocol() {
		if (url == null) {
			return null;
		}
		return url.getProtocol();
	}

	public String getEndPoint() {
		if (isAWS) {
			return endPoint;
		} else {
			if (url == null) {
				return null;
			}
			return url.getHost() + ":" + url.getPort();
		}
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public String getBucket() {
		return bucket;
	}

	public String getPrefix() {
		return prefix;
	}

	public String getRegion() {
		return region;
	}

	public String getMoveSize() {
		return moveSize;
	}

	public void setMountPoint(String mountPoint) {
		this.mountPoint = mountPoint;
	}

	public void setEndPoint(String endPoint) {
		this.endPoint = endPoint;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
