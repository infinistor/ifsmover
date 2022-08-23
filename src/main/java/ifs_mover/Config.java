/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* ifsmover is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version 
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
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
	private String versioning;
	private String sync;
	private String syncCheck;
	private String type;
	
	// for openstack swift
	private String userName;
	private String apiKey;
	private String authEndpoint;
	private String domainId;
	private String domainName;
	private String projectId;
	private String projectName;
	private String container;

	private boolean isAWS;
	private boolean isTargetSync;
	private SyncMode syncMode; 

	private final String MOUNT_POINT = "mountpoint";
	private final String END_POINT = "endpoint";
	private final String REGION = "region";
	private final String ACCESS_KEY = "access";
	private final String SECRET_KEY = "secret";
	private final String BUCKET = "bucket";
	private final String PREFIX = "prefix";
	private final String MOVE_SIZE = "move_size";
	private final String VERSIONING = "versioning";	// ON OFF
	private final String TARGET_SYNC = "sync";
	private final String TARGET_SYNC_MODE = "sync_mode";

	// for openstack swift support
	private final String USER_NAME = "user_name";
	private final String API_KEY = "api_key";
	private final String AUTHENTICATION_SERVICE = "auth_endpoint";
	private final String DOMAIN_ID = "domain_id";
	private final String DOMAIN_NAME = "domain_name";
	private final String PROJECT_ID = "project_id";
	private final String PROJECT_NAME = "project_name";
	private final String CONTAINER = "container";

	private final String PROTOCOL = "http";
	private final String ON = "on";
	private final String SYNC_MODE_ETAG = "etag";
	private final String SYNC_MODE_SIZE = "size";
	private final String SYNC_MODE_EXIST = "exist";

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
		userName = properties.getProperty(USER_NAME);
		apiKey = properties.getProperty(API_KEY);
		authEndpoint = properties.getProperty(AUTHENTICATION_SERVICE);
		domainId = properties.getProperty(DOMAIN_ID);
		domainName = properties.getProperty(DOMAIN_NAME);
		projectId = properties.getProperty(PROJECT_ID);
		projectName = properties.getProperty(PROJECT_NAME);
		container = properties.getProperty(CONTAINER);
		versioning = properties.getProperty(VERSIONING);
		sync = properties.getProperty(TARGET_SYNC);
		syncCheck = properties.getProperty(TARGET_SYNC_MODE);

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

		if (sync != null && !sync.isEmpty()) {
			sync = sync.toLowerCase();
			if (sync.compareTo(ON) == 0) {
				isTargetSync = true;
			} else {
				isTargetSync = false;
			}
		} else {
			isTargetSync = false;
		}

		if (isTargetSync) {
			if (syncCheck != null && !syncCheck.isEmpty()) {
				syncCheck = syncCheck.toLowerCase();
				if (syncCheck.compareTo(SYNC_MODE_ETAG) == 0) {
					syncMode = SyncMode.ETAG;
				} else if (syncCheck.compareTo(SYNC_MODE_SIZE) == 0) {
					syncMode = SyncMode.SIZE;
				} else if (syncCheck.compareTo(SYNC_MODE_EXIST) == 0) {
					syncMode = SyncMode.EXIST;
				} else {
					syncMode = SyncMode.ETAG;
				}
			} else {
				syncMode = SyncMode.ETAG;
			}
		} else {
			syncMode = SyncMode.UNKNOWN;
		}
	}

	public boolean isAWS() {
		return isAWS;
	}

	public boolean isTargetSync() {
		return isTargetSync;
	}

	public SyncMode getSyncMode() {
		return syncMode;
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

	public String getUserName() {
		return userName;
	}

	public String getApiKey() {
		return apiKey;
	}

	public String getAuthEndpoint() {
		return authEndpoint;
	}

	public String getDomainId() {
		return domainId;
	}

	public String getDomainName() {
		return domainName;
	}

	public String getProjectId() {
		return projectId;
	}

	public String getProjectName() {
		return projectName;
	}

	public String getContainer() {
		return container;
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

	public String getVersoning() {
		return versioning;
	}
}
