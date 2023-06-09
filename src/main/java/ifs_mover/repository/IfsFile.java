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
package ifs_mover.repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.Tag;

import ifs_mover.Config;
import ifs_mover.SyncMode;
import ifs_mover.Utils;
import ifs_mover.db.MariaDB;

public class IfsFile implements Repository {
    private static final Logger logger = LoggerFactory.getLogger(IfsFile.class);
    private String jobId;
    private boolean isSource;
    private Config  config;
	private String errCode;
	private String errMessage;
    private String path;

    IfsFile(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public void setConfig(Config config, boolean isSource) {  
        this.config = config;
        this.isSource = isSource;
    }

    @Override
    public int check(String type) {
        path = config.getMountPoint() + config.getPrefix();
			
        File dir = new File(path);
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                errCode = "";
                errMessage = "mountpoint(" + path + ") is not directory.";
                return FILE_PATH_NOT_DIR;
            }
        } else {
            errCode = "";
            errMessage = "mountpoint is not exist.";
            return FILE_PATH_NOT_EXIST;
        }
        return NO_ERROR;
    }

    @Override
    public int init(String type) {
        path = config.getMountPoint() + config.getPrefix();
			
        File dir = new File(path);
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                errCode = "";
                errMessage = "mountpoint(" + path + ") is not directory.";
                return FILE_PATH_NOT_DIR;
            }
        } else {
            errCode = "";
            errMessage = "mountpoint is not exist.";
            return FILE_PATH_NOT_EXIST;
        }
        return NO_ERROR;
    }

    @Override
    public void makeObjectList(boolean isRerun, boolean targetVersioning) {
        if (isRerun) {
            objectListRerun(path);
        } else {
            objectList(path);
        }
    }

    private void objectList(String dirPath) {
        File dir = new File(dirPath);
		File[] files = dir.listFiles();
		String filePath = "";
		Path path;
        
		for (int i = 0; i < files.length; i++) {
			filePath = files[i].getPath();
			path = Paths.get(filePath);
			filePath = FilenameUtils.separatorsToSystem(filePath);
			if (files[i].isDirectory()) {
				Utils.insertMoveObject(jobId, false, "-", 0, filePath, null, null);
				Utils.updateJobInfo(jobId, 0);
				if (!Files.isSymbolicLink(path)) {
					objectList(files[i].getPath());
				}
			}
			else {
				BasicFileAttributes attr = null;
				try {
					attr = Files.readAttributes(path, BasicFileAttributes.class);
				} catch (IOException e) {
					if (files[i].exists()) {
						Utils.insertMoveObject(jobId, true, "", files[i].length(), filePath, null, null);
						Utils.updateJobInfo(jobId, files[i].length());
					} else {
						logger.warn("deleted : {}", files[i].getPath());
					}
					continue;
				}
				
				if (Files.isReadable(path)) {
					Utils.insertMoveObject(jobId, true, attr.lastModifiedTime().toString(), files[i].length(), filePath, null, null);
					Utils.updateJobInfo(jobId, files[i].length());
				} else {
					logger.warn("unreadable file : {}", files[i].getPath());
					Utils.insertMoveObject(jobId, true, "", files[i].length(), filePath, null, null);
					Utils.updateJobInfo(jobId, files[i].length());
				}
			}
		}
        // DBManager.commit();
    }

    private void objectListRerun(String dirPath) {
        File dir = new File(dirPath);
		File[] files = dir.listFiles();
		String filePath = "";
		Path path;
		String mTime = "";
		int state = 0;
		for (int i = 0; i < files.length; i++) {
			filePath = files[i].getPath();
			path = Paths.get(filePath);
			if (files[i].isDirectory()) { 
				// state = DBManager.stateWhenExistObject(jobId, filePath);
                state = Utils.getDBInstance().stateWhenExistObject(jobId, filePath);
				if (state == -1) {
					Utils.insertRerunMoveObject(jobId, false, "-", 0, filePath, null, null, null);
					Utils.updateJobRerunInfo(jobId, 0);
				} else if (state == 3) {
					Utils.updateRerunSkipObject(jobId, filePath);
					Utils.updateJobRerunSkipInfo(jobId, 0);
				} else {
					Utils.updateToMoveObject(jobId, "-", 0, filePath);
					Utils.updateJobRerunInfo(jobId, 0);
				}
				
				if (!Files.isSymbolicLink(path)) {
					objectListRerun(files[i].getPath());
				}
			} else {
				BasicFileAttributes attr = null;
				try {
					attr = Files.readAttributes(path, BasicFileAttributes.class);
				} catch (IOException e) {
					if (files[i].exists()) {
						Utils.insertRerunMoveObject(jobId, true, "", files[i].length(), filePath, null, null, null);
						Utils.updateJobRerunInfo(jobId, files[i].length());
					} else {
						logger.warn("deleted : {}", files[i].getPath());
					}
					continue;
				}
				// state = DBManager.stateWhenExistObject(jobId, filePath);
                state = Utils.getDBInstance().stateWhenExistObject(jobId, filePath);
				if (state == -1) {
					Utils.insertRerunMoveObject(jobId, true, attr.lastModifiedTime().toString(), files[i].length(), filePath, null, null, null);
					Utils.updateJobRerunInfo(jobId, files[i].length());
				} else {
					// mTime = DBManager.getMtime(jobId, filePath);
                    mTime = Utils.getDBInstance().getMtime(jobId, filePath);
					if (mTime != null) {
						if (mTime.compareTo(attr.lastModifiedTime().toString()) == 0) {
							if (state == 3) {
								Utils.updateRerunSkipObject(jobId, filePath);
								Utils.updateJobRerunSkipInfo(jobId, files[i].length());
							} else {
								Utils.updateToMoveObject(jobId, attr.lastModifiedTime().toString(), files[i].length(), filePath);
								Utils.updateJobRerunInfo(jobId, files[i].length());
							}
						} else {
							Utils.updateToMoveObject(jobId, attr.lastModifiedTime().toString(), files[i].length(), filePath);
							Utils.updateJobRerunInfo(jobId, files[i].length());
						}
					} else {
						Utils.updateToMoveObject(jobId, attr.lastModifiedTime().toString(), files[i].length(), filePath);
						Utils.updateJobRerunInfo(jobId, files[i].length());
					}
				}
			}
		}
        // DBManager.commit();
    }

    @Override
    public boolean isVersioning() {
        return false;
    }

    @Override
    public List<String> getBucketList() {
        return null;
    }

    @Override
    public boolean createBuckets(List<String> list) {
        // not support     
        return false;   
    }

    @Override
    public String getErrCode() {
        return errCode;
    }

    @Override
    public String getErrMessage() {
        return errMessage;
    }

    @Override
    public String setPrefix(String path) {
        String newPath = path;
        return newPath;
    }

    @Override
    public String setTargetPrefix(String path) {
        String newPath = path.replace('\\', '/');
        newPath = newPath.replace('\\', '/');
        String str = config.getMountPoint() + config.getPrefix();
        newPath = newPath.substring(str.length());
        if (newPath.startsWith("/")) {
            newPath = newPath.substring(1);
        }

        return newPath;
    }

    @Override
    public ObjectData getObject(String path) {
        ObjectData data = new ObjectData();
        File file = new File(path);
        data.setFile(file);
        data.setSize(file.length());
        return data;
    }

    @Override
    public ObjectData getObject(AmazonS3 client, String bucket, String key, String versionId) {
        return null;
    }

    @Override
    public ObjectData getObject(AmazonS3 client, String bucket, String key, String versionId, long start, long end) {
        return null;
    }

    @Override
    public String getVersioningStatus() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectMetadata getMetadata(AmazonS3 client, String bucket, String key, String versionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectData getObject(AmazonS3 client, String bucket, String key, String versionId, long start) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void makeTargetObjectList(boolean targetVersioning) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean isTargetSync() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public SyncMode getTargetSyncMode() {
        // TODO Auto-generated method stub
        return SyncMode.UNKNOWN;
    }

    @Override
    public AccessControlList getAcl(AmazonS3 client, String bucket, String key, String versionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Tag> getTagging(AmazonS3 client, String bucket, String key, String versionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AmazonS3 createS3Clients() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createS3Clients'");
    }

    @Override
    public void setBucketEncryption(ServerSideEncryptionConfiguration encryption) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setBucketEncryption'");
    }

    @Override
    public ServerSideEncryptionConfiguration getBucketEncryption() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBucketEncryption'");
    }

    @Override
    public String getBucketPolicy() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBucketPolicy'");
    }

    @Override
    public void makeObjectList(boolean isRerun, boolean targetVersioning, String inventoryFileName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'makeObjectList'");
    }
}
