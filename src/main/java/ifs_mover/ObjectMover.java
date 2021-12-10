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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.apache.commons.io.FilenameUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.VersionListing;

public class ObjectMover {
	private static final Logger logger = LoggerFactory.getLogger(ObjectMover.class);
	
	private Config sourceConfig;
	private Config targetConfig;
	private int threadCount;
	private boolean isNAS;
	private boolean isSourceAWS;
	private boolean isTargetAWS;
	private boolean isRerun;
	private boolean isVersioning;
	private String jobId;
	private AmazonS3 sourceS3;
	private AmazonS3 targetS3;
	private String pathNAS;
	private BucketVersioningConfiguration sourceS3VersionConfig;

	private static final int RETRY_COUNT = 3;
	private static final String NO_SUCH_KEY = "NoSuchKey";
	private static final String NOT_FOUND = "Not Found";

	private static final List<Map<String, String>>movedObjectList = new ArrayList<Map<String, String>>();
	private static final List<Map<String, Long>>movedJobList = new ArrayList<Map<String, Long>>();
	private static final List<Map<String, String>>failedObjectList = new ArrayList<Map<String, String>>();
	private static final List<Map<String, Long>>failedJobList = new ArrayList<Map<String, Long>>();

	private static final String ERROR_TARGET_ENDPOINT_IS_NULL = "target endpoint is null";
	
	public static class Builder {
		private Config sourceConfig;
		private Config targetConfig;
		private int threadCount;
		private boolean isNAS;
		private boolean isSourceAWS;
		private boolean isTargetAWS;
		private boolean isRerun;
		private String jobId;
		
		public Builder(String jobId) {
			this.jobId = jobId;
		}
		
		public Builder sourceConfig(Config config) {
			this.sourceConfig = config;
			return this;
		}
		
		public Builder targetConfig(Config config) {
			this.targetConfig = config;
			return this;
		}
		
		public Builder threadCount(int count) {
			this.threadCount = count;
			return this;
		}
		
		public Builder isNAS(boolean isNAS) {
			this.isNAS = isNAS;
			return this;
		}
		
		public Builder isSourceAWS(boolean isAWS) {
			this.isSourceAWS = isAWS;
			return this;
		}
		
		public Builder isTargetAWS(boolean isAWS) {
			this.isTargetAWS = isAWS;
			return this;
		}
		
		public Builder isRerun(boolean isRerun) {
			this.isRerun = isRerun;
			return this;
		}
		
		public ObjectMover build() {
			return new ObjectMover(this);
		}
	}
	
	private ObjectMover(Builder builder) {
		sourceConfig = builder.sourceConfig;
		targetConfig = builder.targetConfig;
		threadCount = builder.threadCount;
		isNAS = builder.isNAS;
		isSourceAWS = builder.isSourceAWS;
		isTargetAWS = builder.isTargetAWS;
		isRerun = builder.isRerun;
		jobId = builder.jobId;
	}
	
	public void check() {
		boolean isSecure;
		
		if (isTargetAWS) {
			isSecure = true;
		} else {
			if (targetConfig.getEndPoint() == null || targetConfig.getEndPoint().isEmpty()) {
				System.out.println(ERROR_TARGET_ENDPOINT_IS_NULL);
				logger.error(ERROR_TARGET_ENDPOINT_IS_NULL);
				System.exit(-1);
			}
			
			if (targetConfig.getEndPointProtocol().compareToIgnoreCase("https") == 0) {
				isSecure = true;
			} else {
				isSecure = false;
			}
		}
		
		if (targetConfig.getBucket() == null || targetConfig.getBucket().isEmpty()) {
			System.out.println("target bucket is null");
			logger.error("target bucket is null");
			System.exit(-1);
		}
		
		try {
			targetS3 = createClient(isTargetAWS, isSecure, targetConfig.getEndPoint(), targetConfig.getAccessKey(), targetConfig.getSecretKey());
		} catch (SdkClientException e) {
			System.out.println("target - Unable to find region");
			System.exit(-1);
		} catch (IllegalArgumentException e) {
			System.out.println("target endpoint is invalid.");
			System.exit(-1);
		}
		
		if (!existBucket(true, false, targetS3, targetConfig.getBucket())) {
			createBucket(true);
		}
		
		if (isNAS) {
			pathNAS = sourceConfig.getMountPoint() + sourceConfig.getPrefix();
			
			File dir = new File(pathNAS);
			if (dir.exists()) {
				if (!dir.isDirectory()) {
					System.out.println("path(" + pathNAS + ") is not directory.");
					logger.error("path({}) is not directory.", pathNAS);
					System.exit(-1);
				}
			} else {
				System.out.println("path(" + pathNAS + ") is not exist.");
				logger.error("path({}) is not exist.", pathNAS);
				System.exit(-1);
			}
			
		} else {
			boolean isSecureSource;
			if (sourceConfig.getEndPoint() != null && !sourceConfig.getEndPoint().isEmpty()) {
				if (isSourceAWS) {
					isSecureSource = true;
				} else {
					if (sourceConfig.getEndPointProtocol().compareToIgnoreCase("https") == 0) {
						isSecureSource = true;
					} else {
						isSecureSource = false;
					}
				}
				try {
					sourceS3 = createClient(isSourceAWS, isSecureSource, sourceConfig.getEndPoint(), sourceConfig.getAccessKey(), sourceConfig.getSecretKey());
				} catch (SdkClientException e) {
					System.out.println("source - Unable to find region");
					System.exit(-1);
				} catch (IllegalArgumentException e) {
					System.out.println("source endpoint is invalid.");
					System.exit(-1);
				}
			} else {
				System.out.println("source : enter a value for endpoint or region.");
				logger.error("source : enter a value for endpoint or region.");
				System.exit(-1);
			}
			
			if (sourceConfig.getBucket() == null || sourceConfig.getBucket().isEmpty()) {
				System.out.println("source : bucket is null");
				logger.error("source : bucket is null");
				DBManager.insertErrorJob(jobId, "source : bucket is null");
				System.exit(-1);
			}
			
			if (!existBucket(true, true, sourceS3, sourceConfig.getBucket())) {
				System.out.println("source : bucket(" + sourceConfig.getBucket() + ") does not exist.");
				logger.error("source : bucket({}) does not exist.", sourceConfig.getBucket());
				DBManager.insertErrorJob(jobId, "source : bucket(" + sourceConfig.getBucket() + ") does not exist.");
				System.exit(-1);
			}
			
			if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
				ListObjectsRequest request = new ListObjectsRequest().withBucketName(sourceConfig.getBucket()).withPrefix(sourceConfig.getPrefix());
				
				ObjectListing result = sourceS3.listObjects(request);
				if (result.getObjectSummaries().isEmpty()) {
					System.out.println("source : bucket(" + sourceConfig.getBucket() + "), prefix(" + sourceConfig.getPrefix() + ") doesn't have an object.");
					logger.error("source : bucket({}), prefix({}) doesn't have an object.", sourceConfig.getBucket(), sourceConfig.getPrefix());
					DBManager.insertErrorJob(jobId, "source : bucket(" + sourceConfig.getBucket() + "), prefix(" + sourceConfig.getPrefix() + ") doesn't have an object.");
					System.exit(-1);
				}
			}
		}
	}
	
	public void init() {
		boolean isSecure;
		
		if (isTargetAWS) {
			isSecure = true;
		} else {
			if (targetConfig.getEndPoint() == null || targetConfig.getEndPoint().isEmpty()) {
				System.out.println(ERROR_TARGET_ENDPOINT_IS_NULL);
				logger.error(ERROR_TARGET_ENDPOINT_IS_NULL);
				DBManager.insertErrorJob(jobId, ERROR_TARGET_ENDPOINT_IS_NULL);
				System.exit(-1);
			}
			
			if (targetConfig.getEndPointProtocol().compareToIgnoreCase("https") == 0) {
				isSecure = true;
			} else {
				isSecure = false;
			}
		}
		
		if (targetConfig.getBucket() == null || targetConfig.getBucket().isEmpty()) {
			System.out.println("target bucket is null");
			logger.error("target bucket is null");
			DBManager.insertErrorJob(jobId, "target bucket is null");
			System.exit(-1);
		}
		
		try {
			targetS3 = createClient(isTargetAWS, isSecure, targetConfig.getEndPoint(), targetConfig.getAccessKey(), targetConfig.getSecretKey());
		} catch (SdkClientException e) {
			System.out.println("Unable to find region");
			logger.error("Unable to find region");
			DBManager.insertErrorJob(jobId, "Unable to find region");
			System.exit(-1);
		} catch (IllegalArgumentException e) {
			System.out.println("target endpoint is invalid.");
			logger.error("target endpoint is invalid.");
			DBManager.insertErrorJob(jobId, "target endpoint is invalid.");
			System.exit(-1);
		}
		
		if (!existBucket(false, false, targetS3, targetConfig.getBucket())) {
			createBucket(false);
		}

		String prePath = sourceConfig.getMountPoint() + sourceConfig.getPrefix();
		if (!prePath.endsWith("/")) {
			prePath += "/";
		}
		
		if (isNAS) {
			pathNAS = sourceConfig.getMountPoint() + sourceConfig.getPrefix();
			
			File dir = new File(pathNAS);
			if (dir.exists()) {
				if (!dir.isDirectory()) {
					logger.error("path({}) is not a directory.", pathNAS);
					DBManager.insertErrorJob(jobId, "source mountpoint/prefix is not a directory.");
					System.exit(-1);
				}
			} else {
				logger.error("path({}) is not exist.", pathNAS);
				DBManager.insertErrorJob(jobId, "source mountpoint/prefix is not exist.");
				System.exit(-1);
			}

			if (isRerun) {
				makeObjectListNasRerun(pathNAS);
			} else {
				makeObjectListNas(pathNAS);
			}
		} else {
			boolean isSecureSource;
			if (sourceConfig.getEndPoint() != null || !sourceConfig.getEndPoint().isEmpty()) {
				if (isSourceAWS) {
					isSecureSource = true;
				} else {
					if (sourceConfig.getEndPointProtocol().compareToIgnoreCase("https") == 0) {
						isSecureSource = true;
					} else {
						isSecureSource = false;
					}
				}
				
				try {
					sourceS3 = createClient(isSourceAWS, isSecureSource, sourceConfig.getEndPoint(), sourceConfig.getAccessKey(), sourceConfig.getSecretKey());
				} catch (SdkClientException e) {
					System.out.println("Unable to find region");
					logger.error("Unable to find region");
					DBManager.insertErrorJob(jobId, "Unable to find region");
					System.exit(-1);
				} catch (IllegalArgumentException e) {
					System.out.println("source endpoint is invalid.");
					logger.error("source endpoint is invalid.");
					DBManager.insertErrorJob(jobId, "source endpoint is invalid.");
					System.exit(-1);
				}
			} else {
				logger.error("source : enter a value for endpoint or region.");
				DBManager.insertErrorJob(jobId, "source : enter a value for endpoint or region.");
				System.out.println("source : enter a value for endpoint or region.");
				System.exit(-1);
			}
			
			if (sourceConfig.getBucket() == null || sourceConfig.getBucket().isEmpty()) {
				logger.error("source : bucket is null");
				DBManager.insertErrorJob(jobId, "source : bucket is null");
				System.exit(-1);
			}
			
			if (!existBucket(false, false, sourceS3, sourceConfig.getBucket())) {
				logger.error("source : bucket({}) does not exist.", sourceConfig.getBucket());
				DBManager.insertErrorJob(jobId, "source : bucket(" + sourceConfig.getBucket() + ") does not exist.");
				System.exit(-1);
			}

			try {
				sourceS3VersionConfig = sourceS3.getBucketVersioningConfiguration(sourceConfig.getBucket());
			} catch (AmazonServiceException ase) {
				logger.warn("{} {}", ase.getErrorCode(), ase.getErrorMessage());
			} catch (SdkClientException ace) {
				logger.warn("{}", ace.getMessage());
			}
			
			if (sourceS3VersionConfig.getStatus().equals(BucketVersioningConfiguration.OFF)) {
				isVersioning = false;
			} else {
				isVersioning = true;
				targetS3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(targetConfig.getBucket(), 
					new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
			}
			
			if (isRerun) {
				makeObjectVersionListRerun();
			} else {
				makeObjectVersionList();
			}
		}
		
		if (isRerun) {
			DBManager.deleteCheckObjects(jobId);
		}
	}

	private void retryUpdateObjectVersionMove(String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.updateObjectVersionMove(jobId, path, versionId)) {
				return;
			}
		}

		logger.error("failed updateObjectVersionMove. path={}", path);
	}

	public void startVersion() {		
		String finalPath = "";
		String path = "";
		String versionId = "";
		boolean isDoneDistribute = false;
		long sequence = 0;
		long limit = 100;

		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		List<Map<String, String>> list = null;
		List<Map<String, String>> jobList = null;

		String prefix = targetConfig.getPrefix();
		if (prefix != null && !prefix.isEmpty()) {
			StringBuilder bld = new StringBuilder();
			String[] tmpArr = prefix.split("/");
			for (String str : tmpArr) {
				if (!str.isEmpty()) {
					bld.append(str);
					bld.append("/");
				}
			}
			String prefixFinal = bld.toString();
			if (prefixFinal.length() >= 1) {
				try {
					ObjectMetadata meta = new ObjectMetadata();
					meta.setContentLength(0L);
					InputStream is = new ByteArrayInputStream(new byte[0]);
					PutObjectRequest req = new PutObjectRequest(targetConfig.getBucket(), prefixFinal, is, meta);
					targetS3.putObject(req);
					logger.info("make target prefix {}", prefixFinal);
				} catch (AmazonServiceException ase) {
					logger.warn("{} {} {}", prefixFinal, ase.getErrorCode(), ase.getErrorMessage());
				} catch (AmazonClientException ace) {
					logger.warn("{} {}", prefixFinal, ace.getMessage());
				}
			}
		}

		long maxSequence = DBManager.getMaxSequence(jobId);

		while (true) {
			if (!isDoneDistribute) {
				list = DBManager.getToMoveObjectsInfo(jobId, sequence, limit);
				if (list.isEmpty() && sequence >= maxSequence) {
					if (jobList != null && !jobList.isEmpty()) {
						Mover mover = new Mover(jobList);
						executor.execute(mover);
					}
					isDoneDistribute = true;
					executor.shutdown();
					continue;
				}
				sequence += limit;
				
				for (Map<String, String> jobInfo : list) {
					path = jobInfo.get("path");
					versionId = jobInfo.get("versionId");

					retryUpdateObjectVersionMove(path, versionId);
					
					if (path.compareTo(finalPath) != 0) {
						finalPath = path;
						if (jobList == null) {
							jobList = new ArrayList<Map<String, String>>();
							jobList.add(jobInfo);
						} else {
							Mover mover = new Mover(jobList);
							executor.execute(mover);
							jobList = new ArrayList<Map<String, String>>();
							jobList.add(jobInfo);
						}
					} else {
						if (jobList == null) {
							jobList = new ArrayList<Map<String, String>>();
						}
						jobList.add(jobInfo);
					}
				}
			} 
			
			if (isDoneDistribute && executor.isTerminated()) {
				for (Map<String, String> movedObject : movedObjectList) {
					path = movedObject.get("path");
					versionId = movedObject.get("versionId");

					DBManager.updateObjectVersionMoveComplete(jobId, path, versionId);
				}

				for (Map<String, Long> movedJob : movedJobList) {
					long size = movedJob.get("size").longValue();

					DBManager.updateJobMoved(jobId, size);
				}

				for (Map<String, String> failedObject : failedObjectList) {
					path = failedObject.get("path");
					versionId = failedObject.get("versionId");

					DBManager.updateObjectVersionMoveEventFailed(jobId, path, versionId, "", "retry failure");
				}

				for (Map<String, Long> failedJob : failedJobList) {
					long size = failedJob.get("size").longValue();
					DBManager.updateJobFailedInfo(jobId, size);
				}

				if (isVersioning) {
					try {
						targetS3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(targetConfig.getBucket(), sourceS3VersionConfig));
					} catch (AmazonServiceException ase) {
						logger.warn("{} {}", ase.getErrorCode(), ase.getErrorMessage());
					} catch (SdkClientException  ace) {
						logger.warn("{}", ace.getMessage());
					}
				}

				return;
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}
	
	private void retryInsertMoveObject(boolean isFile, String mTime, long size, String path) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.insertMoveObject(jobId, isFile, mTime, size, path)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed insertMoveObject. path={}", path);
	}

	private void retryInsertRerunMoveObject(boolean isFile, String mTime, long size, String path) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.insertRerunMoveObject(jobId, isFile, mTime, size, path)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed insertRerunMoveObject. path={}", path);
	}

	private void retryUpdateJobInfo(long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.updateJobInfo(jobId, size)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateJobInfo. size={}", size);
	}

	private void retryUpdateJobRerunInfo(long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.updateJobRerunInfo(jobId, size)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateJobInfo. size={}", size);
	}

	private void retryUpdateRerunSkipObject(String path) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.updateRerunSkipObject(jobId, path)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateRerunSkipObject. path={}", path);
	}

	private void retryUpdateJobRerunSkipInfo(long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.updateJobRerunSkipInfo(jobId, size)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateJobRerunSkipInfo. size={}", size);
	}

	private void retryUpdateToMoveObject(String mTime, long size, String path) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.updateToMoveObject(jobId, mTime, size, path)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateToMoveObject. path={}", path);
	}

	private void makeObjectListNas(String dirPath) {
		File dir = new File(dirPath);
		File[] files = dir.listFiles();
		String filePath = "";
		Path path;
		
		for (int i = 0; i < files.length; i++) {
			filePath = files[i].getPath();
			path = Paths.get(filePath);
			filePath = FilenameUtils.separatorsToSystem(filePath);
			if (files[i].isDirectory()) {
				retryInsertMoveObject(false, "-", 0, filePath);
				retryUpdateJobInfo(0);
				if (!Files.isSymbolicLink(path)) {
					makeObjectListNas(files[i].getPath());
				}
			}
			else {
				BasicFileAttributes attr = null;
				try {
					attr = Files.readAttributes(path, BasicFileAttributes.class);
				} catch (IOException e) {
					if (files[i].exists()) {
						retryInsertMoveObject(true, "", files[i].length(), filePath);
						retryUpdateJobInfo(files[i].length());
					} else {
						logger.warn("deleted : {}", files[i].getPath());
					}
					continue;
				}
				
				if (Files.isReadable(path)) {
					retryInsertMoveObject(true, attr.lastModifiedTime().toString(), files[i].length(), filePath);
					retryUpdateJobInfo(files[i].length());
				} else {
					logger.warn("unreadable file : {}", files[i].getPath());
					retryInsertMoveObject(true, "", files[i].length(), filePath);
					retryUpdateJobInfo(files[i].length());
				}
			}
		}
	}
	
	private void makeObjectListNasRerun(String dirPath) {
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
				state = DBManager.stateWhenExistObject(jobId, filePath);
				if (state == -1) {
					retryInsertRerunMoveObject(false, "-", 0, filePath);
					retryUpdateJobRerunInfo(0);
				} else if (state == 3) {
					retryUpdateRerunSkipObject(filePath);
					retryUpdateJobRerunSkipInfo(0);
				} else {
					retryUpdateToMoveObject("-", 0, filePath);
					retryUpdateJobRerunInfo(0);
				}
				
				if (!Files.isSymbolicLink(path)) {
					makeObjectListNasRerun(files[i].getPath());
				}
			}
			else {
				BasicFileAttributes attr = null;
				try {
					attr = Files.readAttributes(path, BasicFileAttributes.class);
				} catch (IOException e) {
					if (files[i].exists()) {
						retryInsertRerunMoveObject(true, "", files[i].length(), filePath);
						retryUpdateJobRerunInfo(files[i].length());
					} else {
						logger.warn("deleted : {}", files[i].getPath());
					}
					continue;
				}
				state = DBManager.stateWhenExistObject(jobId, filePath);
				if (state == -1) {
					retryInsertRerunMoveObject(true, attr.lastModifiedTime().toString(), files[i].length(), filePath);
					retryUpdateJobRerunInfo(files[i].length());
				} else {
					mTime = DBManager.getMtime(jobId, filePath);
					if (mTime != null) {
						if (mTime.compareTo(attr.lastModifiedTime().toString()) == 0) {
							if (state == 3) {
								retryUpdateRerunSkipObject(filePath);
								retryUpdateJobRerunSkipInfo(files[i].length());
							} else {
								retryUpdateToMoveObject(attr.lastModifiedTime().toString(), files[i].length(), filePath);
								retryUpdateJobRerunInfo(files[i].length());
							}
						} else {
							retryUpdateToMoveObject(attr.lastModifiedTime().toString(), files[i].length(), filePath);
							retryUpdateJobRerunInfo(files[i].length());
						}
					} else {
						retryUpdateToMoveObject(attr.lastModifiedTime().toString(), files[i].length(), filePath);
						retryUpdateJobRerunInfo(files[i].length());
					}
				}
			}
		}
	}
	
	private AmazonS3 createClient(boolean isAWS, boolean isSecure, String URL, String AccessKey, String SecretKey) throws SdkClientException, IllegalArgumentException{
		ClientConfiguration config;

		if (isSecure) {
			config = new ClientConfiguration().withProtocol(Protocol.HTTPS);
		} else {
			config = new ClientConfiguration().withProtocol(Protocol.HTTP);
		}

		config.setSignerOverride("AWSS3V4SignerType");
		AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();

		clientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(AccessKey, SecretKey)));
		
		if (isAWS) {
			clientBuilder.setRegion(URL);
		} else {
			clientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(URL, ""));
		}
		
		clientBuilder.setClientConfiguration(config);
		clientBuilder.setPathStyleAccessEnabled(true);
		
		return clientBuilder.build();
	}
	
	private boolean existBucket(boolean isCheck, boolean isSource, AmazonS3 s3, String bucket) {
		boolean isExist = false;
		try {
			isExist = s3.doesBucketExistV2(bucket);
		} catch (AmazonServiceException ase) {
			String errCode = ase.getErrorCode();
			String statusCode = String.valueOf(ase.getStatusCode());
			logger.error("errCode : {}, statusCode : {}", errCode, statusCode);
			switch (errCode) {
			case "InvalidAccessKeyId":
				if (isCheck) {
					if (isSource) {
						System.out.println("Error : source - The access key is invalid.");
						logger.error("source - The access key is invalid.");
					} else {
						System.out.println("Error : target - The access key is invalid.");
						logger.error("target - The access key is invalid.");
					}
				} else {
					if (isSource) {
						logger.error("source - The access key is invalid.");
						DBManager.insertErrorJob(jobId, "source - The access key is invalid.");
					} else {
						logger.error("target - The access key is invalid.");
						DBManager.insertErrorJob(jobId, "target - The access key is invalid.");
					}
				}
				
				break;
				
			case "SignatureDoesNotMatch":
				if (isCheck) {
					if (isSource) {
						System.out.println("Error : source - The secret key is invalid.");
						logger.error("source - The secret key is invalid.");
					} else {
						System.out.println("Error : target - The secret key is invalid.");
						logger.error("target - The secret key is invalid.");
					}
				} else {
					if (isSource) {
						logger.error("source - The secret key is invalid.");
						DBManager.insertErrorJob(jobId, "source - The secret key is invalid.");
					} else {
						logger.error("target - The secret key is invalid.");
						DBManager.insertErrorJob(jobId, "target - The secret key is invalid.");
					}
				}
				
				break;
			}
			System.exit(-1);
        } catch (AmazonClientException ace) {
        	logger.error(ace.getMessage());
        	if (isCheck) {
        		if (isSource) {
        			System.out.println("Error : source - " + ace.getMessage());
        		} else {
        			System.out.println("Error : target - " + ace.getMessage());
        		}
        		
        		System.exit(-1);
        	}
        	
        	if (isSource) {
        		DBManager.insertErrorJob(jobId, "source - " + ace.getMessage());
        	} else {
        		DBManager.insertErrorJob(jobId, "target - " + ace.getMessage());
        	}
        	System.exit(-1);
        }
		
		return isExist;
	}
	
	private void createBucket(boolean isCheck) {
		try {
			targetS3.createBucket(targetConfig.getBucket());
			if (isCheck) {
				targetS3.deleteBucket(targetConfig.getBucket());
			}
		} catch (AmazonServiceException ase) {
			if (ase.getErrorCode().compareToIgnoreCase("BucketAlreadyOwnedByYou") == 0) {
				return;
			} else if (ase.getErrorCode().compareToIgnoreCase("BucketAlreadyExists") == 0) {
				return;
			}

			if (isCheck) {
				System.out.println("Error : " + ase.getMessage());
				System.exit(-1);
			}
			
			logger.error("error code : {}", ase.getErrorCode());
			logger.error(ase.getMessage());
			DBManager.insertErrorJob(jobId, ase.getMessage());
			System.exit(-1);
        }
	}

	private void retryInsertMoveObjectVersioning(boolean isFile, String mTime, long size, String path, String versionId, boolean isDelete, boolean isLatest) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.insertMoveObjectVersioning(jobId, isFile, mTime, size, path, versionId, isDelete, isLatest)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed insertMoveObjectVersioning. path={}", path);
	}

	private void retryInsertRerunMoveObjectVersion(boolean isFile, String mTime, long size, String path, String versionId, boolean isDelete, boolean isLatest) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.insertRerunMoveObjectVersion(jobId, isFile, mTime, size, path, versionId, isDelete, isLatest)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed insertRerunMoveObjectVersion. path={}", path);
	}

	private void retryUpdateRerunSkipObjectVersion(String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.updateRerunSkipObjectVersion(jobId, path, versionId)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateRerunSkipObjectVersion. path={}", path);
	}

	private void retryUpdateToMoveObjectVersion(String mTime, long size, String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.updateToMoveObjectVersion(jobId, mTime, size, path, versionId)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateToMoveObjectVersion. path={}", path);
	}

	private void makeObjectVersionList() {
		long count = 0;

		try {
			if (isVersioning) {
				ListVersionsRequest request = null;
				if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
					request = new ListVersionsRequest().withBucketName(sourceConfig.getBucket()).withPrefix(sourceConfig.getPrefix());
				} else {
					request = new ListVersionsRequest().withBucketName(sourceConfig.getBucket()).withPrefix("");
				}
				VersionListing listing = null;
				do {
					listing = sourceS3.listVersions(request);
					for (S3VersionSummary versionSummary : listing.getVersionSummaries()) {
						count++;
						retryInsertMoveObjectVersioning(versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/', 
							versionSummary.getLastModified().toString(), 
							versionSummary.getSize(), 
							versionSummary.getKey(), 
							versionSummary.getVersionId(), 
							versionSummary.isDeleteMarker(), 
							versionSummary.isLatest());
						retryUpdateJobInfo(versionSummary.getSize());
					}
					request.setKeyMarker(listing.getNextKeyMarker());
					request.setVersionIdMarker(listing.getNextVersionIdMarker());
				} while (listing.isTruncated());
			} else {
				ListObjectsRequest request = null;
				if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
					request = new ListObjectsRequest().withBucketName(sourceConfig.getBucket()).withPrefix(sourceConfig.getPrefix());
				} else {
					request = new ListObjectsRequest().withBucketName(sourceConfig.getBucket()).withPrefix("");
				}
				ObjectListing result;
				do {
					result = sourceS3.listObjects(request);
					for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {		
						count++;
						retryInsertMoveObject(objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/', 
							objectSummary.getLastModified().toString(), 
							objectSummary.getSize(), 
							objectSummary.getKey());
						retryUpdateJobInfo(objectSummary.getSize());
					}
					request.setMarker(result.getNextMarker());
				} while (result.isTruncated());
			}
		} catch (AmazonServiceException ase) {
			logger.error("Error code : {}", ase.getErrorCode());
			logger.error("Error message : {}", ase.getErrorMessage());
			DBManager.insertErrorJob(jobId, ase.getErrorCode() + "," + ase.getErrorMessage());
			System.exit(-1);
        } catch (AmazonClientException ace) {
        	logger.error(ace.getMessage());
        	DBManager.insertErrorJob(jobId, ace.getMessage());
        	System.exit(-1);
        }
		
		logger.info("listing size : {}", count);
		
		if (count == 0) {
			logger.error("Doesn't havn an object.");
			DBManager.insertErrorJob(jobId, "Doesn't havn an object.");
			System.exit(-1);
		}
	}

	private void makeObjectVersionListRerun() {
		int state = 0;
		try {
			if (isVersioning) {
				ListVersionsRequest request = null;
				if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
					request = new ListVersionsRequest().withBucketName(sourceConfig.getBucket()).withPrefix(sourceConfig.getPrefix());
				} else {
					request = new ListVersionsRequest().withBucketName(sourceConfig.getBucket()).withPrefix("");
				}

				VersionListing listing = null;
				do {
					listing = sourceS3.listVersions(request);
					for (S3VersionSummary versionSummary : listing.getVersionSummaries()) {
						String versionId = versionSummary.getVersionId();
						if (versionId.compareToIgnoreCase("null") == 0) {
							versionId = "";
						}
						Map<String, String> info = DBManager.infoExistObjectVersion(jobId, versionSummary.getKey(), versionId);
						if (info.isEmpty()) {
							retryInsertRerunMoveObjectVersion(versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/', 
								versionSummary.getLastModified().toString(), 
								versionSummary.getSize(), 
								versionSummary.getKey(), 
								versionId, 
								versionSummary.isDeleteMarker(), 
								versionSummary.isLatest());
							retryUpdateJobRerunInfo(versionSummary.getSize());
						} else {
							state = Integer.parseInt(info.get("object_state"));
							String mTime = info.get("mtime");
							if (state == 3 && mTime.compareTo(versionSummary.getLastModified().toString()) == 0) {	
								retryUpdateRerunSkipObjectVersion(versionSummary.getKey(), versionId);
								retryUpdateJobRerunSkipInfo(versionSummary.getSize());
							} else {
								retryUpdateToMoveObjectVersion(versionSummary.getLastModified().toString(), versionSummary.getSize(), versionSummary.getKey(), versionId);
								retryUpdateJobRerunInfo(versionSummary.getSize());
							}
						}
					}
					request.setKeyMarker(listing.getNextKeyMarker());
					request.setVersionIdMarker(listing.getNextVersionIdMarker());
				} while (listing.isTruncated());
			} else {
				ListObjectsRequest request = null;
				if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
					request = new ListObjectsRequest().withBucketName(sourceConfig.getBucket()).withPrefix(sourceConfig.getPrefix());
				} else {
					request = new ListObjectsRequest().withBucketName(sourceConfig.getBucket()).withPrefix("");
				}

				ObjectListing result;
				do {
					result = sourceS3.listObjects(request);
					for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
						Map<String, String> info = DBManager.infoExistObject(jobId, objectSummary.getKey());
						if (info.isEmpty()) {
							retryInsertRerunMoveObject(objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/', 
								objectSummary.getLastModified().toString(), 
								objectSummary.getSize(), 
								objectSummary.getKey());
							retryUpdateJobRerunInfo(objectSummary.getSize());
						} else {
							state = Integer.parseInt(info.get("object_state"));
							String mTime = info.get("mtime");
							if (state == 3 && mTime.compareTo(objectSummary.getLastModified().toString()) == 0) {
								retryUpdateRerunSkipObject(objectSummary.getKey());
								retryUpdateJobRerunSkipInfo(objectSummary.getSize());
							} else {
								retryUpdateToMoveObject(objectSummary.getLastModified().toString(), objectSummary.getSize(), objectSummary.getKey());
								retryUpdateJobRerunInfo(objectSummary.getSize());
							}
						}
					}
					request.setMarker(result.getNextMarker());
				} while (result.isTruncated());
			}
		} catch (AmazonServiceException ase) {
			logger.error("Error code : {}", ase.getErrorCode());
			logger.error("Error message : {}", ase.getErrorMessage());
			DBManager.insertErrorJob(jobId, ase.getErrorCode() + "," + ase.getErrorMessage());
			System.exit(-1);
        } catch (AmazonClientException ace) {
        	logger.error(ace.getMessage());
        	DBManager.insertErrorJob(jobId, ace.getMessage());
        	System.exit(-1);
        }
	}

	private static synchronized void addMovedObjectList(String path, String versionId) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("path", path);
		map.put("versionId", versionId);

		movedObjectList.add(map);
	}

	private static synchronized void addMovedJobList(long size) {
		Map<String, Long> map = new HashMap<String, Long>();
		map.put("size", Long.valueOf(size));

		movedJobList.add(map);
	}

	private static synchronized void addFailedObjectList(String path, String versionId) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("path", path);
		map.put("versionId", versionId);

		failedObjectList.add(map);
	}

	private static synchronized void addFailedJobList(long size) {
		Map<String, Long> map = new HashMap<String, Long>();
		map.put("size", Long.valueOf(size));

		failedJobList.add(map);
	}
	
	class Mover implements Runnable {
		final Logger logger = LoggerFactory.getLogger(Mover.class);
		List<Map<String, String>> list;

		private String path;
		private String latestPath;
		private long size;
		private long latestSize;
		private boolean isFile;
		private boolean latestIsFile;
		private String versionId;
		private String latestVersionId;
		private boolean isDelete;
		private boolean latestIsDelete;
		private boolean isLatest;

		private boolean isFault;
		private boolean isDoneLatest;
				
		public Mover(List<Map<String, String>> list) {
			this.list = list;
		}

		Mover(String path, long size, boolean isFile, String versionId, boolean isDelete) {
			this.path = path;
			this.size = size;
			this.isFile = isFile;
			this.versionId = versionId;
			this.isDelete = isDelete;
		}
		
		private boolean uploadFile(boolean isSize, String fileName, File file) {
			String logPath = fileName;
			try {	
				fileName = fileName.replace('\\', '/');
				String str = sourceConfig.getMountPoint() + sourceConfig.getPrefix();
				fileName = fileName.substring(str.length());
				if (fileName.startsWith("/")) {
					fileName = fileName.substring(1);
				}
				String prefix = targetConfig.getPrefix();
				if (prefix != null && !prefix.isEmpty()) {
					if (prefix.startsWith("/")) {
						prefix = prefix.substring(1);
					}
					if (!prefix.endsWith("/")) {
						prefix = prefix + "/";
					}
					fileName = prefix + fileName;
				}
				
				if (isSize) {
					targetS3.putObject(new PutObjectRequest(targetConfig.getBucket(), fileName, file));
				} else {
					ObjectMetadata meta = new ObjectMetadata();
					meta.setContentLength(0L);
					InputStream is = new ByteArrayInputStream(new byte[0]);
					PutObjectRequest req = new PutObjectRequest(targetConfig.getBucket(), fileName, is, meta);
					targetS3.putObject(req);
				}
				logger.info("move success : {}", logPath);
			} catch (AmazonServiceException ase) {
				logger.warn("{} {} {}", logPath, ase.getErrorCode(), ase.getErrorMessage());
				return false;
	        } catch (AmazonClientException ace) {
	        	logger.warn("{} {}", logPath, ace.getMessage());
	        	return false;
	        }
			
			return true;
		}
		
		private boolean uploadDirectory(String path) {
			String logPath = path;
			try {
				path = path.substring((sourceConfig.getMountPoint() + sourceConfig.getPrefix()).length());
				
				if (path.startsWith("/")) {
					path = path.substring(1);
				}
								
				path = path.replace('\\', '/');
				String prefix = targetConfig.getPrefix();
				if (prefix != null && !prefix.isEmpty()) {
					if (prefix.startsWith("/")) {
						prefix = prefix.substring(1);
					}
					if (!prefix.endsWith("/")) {
						prefix = prefix + "/";
					}
					path = prefix + path;
				}
				
				if (!path.endsWith("/")) {
					path += "/";
				}
				
				ObjectMetadata meta = new ObjectMetadata();
				meta.setContentLength(0L);
				InputStream is = new ByteArrayInputStream(new byte[0]);
				PutObjectRequest req = new PutObjectRequest(targetConfig.getBucket(), path, is, meta);
				targetS3.putObject(req);
				logger.info("move success : {}", logPath);
			} catch (AmazonServiceException ase) {
				logger.warn("{} {} {}", logPath, ase.getErrorCode(), ase.getErrorMessage());
				return false;
	        } catch (AmazonClientException ace) {
	        	logger.warn("{} {}", logPath, ace.getMessage());
	        	return false;
	        }
			
			return true;
		}
		
		private boolean moveObject(String path, boolean isDelete, boolean isFile, String versionId) {
			String sourcePath = path;
			String targetObject = path;

			try {
				if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
					path = path.substring(sourceConfig.getPrefix().length());
					if (path.startsWith("/")) {
						path = path.substring(1);
					}
				}
				
				String prefix = targetConfig.getPrefix();
				if (prefix != null && !prefix.isEmpty()) {
					StringBuilder bld = new StringBuilder();
					String[] tmpArr = prefix.split("/");
					for (String str : tmpArr) {
						if (!str.isEmpty()) {
							bld.append(str);
							bld.append("/");
						}
					}
					String prefixFinal = bld.toString();
					if (prefixFinal.length() >= 1) {
						targetObject = prefixFinal + path;
					}
				}
				
				if (isDelete) {
					targetS3.deleteObject(targetConfig.getBucket(), targetObject);
					logger.info("delete success : {}", sourcePath);
				} else {
					InputStream is = null;
					ObjectMetadata meta = null;
					GetObjectRequest getObjectRequest = null;
					S3Object objectData = null;

					if (isFile) {
						if (versionId == null) {
							getObjectRequest = new GetObjectRequest(sourceConfig.getBucket(), sourcePath);
						} else {
							getObjectRequest = new GetObjectRequest(sourceConfig.getBucket(), sourcePath).withVersionId(versionId);
						}
						objectData = sourceS3.getObject(getObjectRequest);
						is = objectData.getObjectContent();
						meta = objectData.getObjectMetadata();
					} else {
						GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(sourceConfig.getBucket(), sourcePath);
						meta = sourceS3.getObjectMetadata(getObjectMetadataRequest);
						is = new ByteArrayInputStream(new byte[0]);
					}

					PutObjectRequest putObjectRequest;
					putObjectRequest = new PutObjectRequest(targetConfig.getBucket(), targetObject, is, meta);
					putObjectRequest.getRequestClientOptions().setReadLimit(1024 * 1024 * 1024);
					targetS3.putObject(putObjectRequest);
					if (versionId != null && !versionId.isEmpty()) {
						if (versionId.compareToIgnoreCase("null") == 0) {
							logger.info("move success : {}", sourcePath);
						} else {
							logger.info("move success : {}:{}", sourcePath, versionId);
						}
					} else {
						logger.info("move success : {}", sourcePath);
					}
				}
			} catch (AmazonServiceException ase) {
				if (ase.getErrorCode().compareToIgnoreCase(NO_SUCH_KEY) == 0) {
					if (versionId == null) {
						logger.warn("{} {}", sourcePath, ase.getErrorMessage());
					} else {
						logger.warn("{}:{} {}", sourcePath, versionId, ase.getErrorMessage());
					}
				} else if (ase.getErrorMessage().contains(NOT_FOUND)) {
					logger.warn("{} {}", sourcePath, ase.getErrorMessage());
				} else {
					logger.warn("{} {}- {}", sourcePath, ase.getErrorCode(), ase.getErrorMessage());
				}
				return false;
	        } catch (AmazonClientException ace) {
	        	logger.warn("{} {}", sourcePath, ace.getMessage());
	        	return false;
	        }
			
			return true;
		}
		
		private void retryUploadFile(boolean isSize, String path, File file) {
			for (int i = 0; i < RETRY_COUNT; i++) {
				if (uploadFile(isSize, path, file)) {
					return;
				}
			}
			
			isFault = true;
		}
		
		private void retryUploadDirectory(String path) {
			for (int i = 0; i < RETRY_COUNT; i++) {
				if (uploadDirectory(path)) {
					return;
				}
			}
			
			isFault = true;
		}
		
		private void retryMoveObject(String path, boolean isDelete, boolean isFile, String versionId) {
			for (int i = 0; i < RETRY_COUNT; i++) {
				if (moveObject(path, isDelete, isFile, versionId)) {
					return;
				}
			}
			
			isFault = true;
		}
		
		private void retryUpdateJobMoved(long size) {
			for (int i = 0; i < RETRY_COUNT; i++) {
				if (DBManager.updateJobMoved(jobId, size)) {
					return;
				} else {
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}
				}
			}
			logger.error("failed update move info to Job table(size={})", size);
			addMovedJobList(size);
		}

		private void retryUpdateObjectMoveEvent(String path, String versionId) {
			for (int i = 0; i < RETRY_COUNT; i++) {
				if (DBManager.updateObjectVersionMoveComplete(jobId, path, versionId)) {
					return;
				} else {
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}
				}
			}
			logger.error("failed update move info. {}:{}", path, versionId);
			addMovedObjectList(path, versionId);
		}

		private void retryUpdateObjectVersionMoveEventFailed(String path, String versionId, String errorCode, String errorDesc) {
			for (int i = 0; i < RETRY_COUNT; i++) {
				if (DBManager.updateObjectVersionMoveEventFailed(jobId, path, versionId, "", "retry failure")) {
					return;
				} else {
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}
				}
			}
			logger.error("failed UpdateObjectVersionMoveEventFailed. {}:{}", path, versionId);
			addFailedObjectList(path, versionId);
		}

		private void retryUpdateJobFailedInfo(long size) {
			for (int i = 0; i < RETRY_COUNT; i++) {
				if (DBManager.updateJobFailedInfo(jobId, size)) {
					return;
				} else {
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}
				}
			}
			logger.error("failed UpdateJobFailedInfo. {}", size);
			addFailedJobList(size);
		}

		@Override
		public void run() {
			MDC.clear();
			MDC.put("logFileName", "ifs_mover." + jobId + ".log");
			isFault = false;
			isDoneLatest = true;
			for (Map<String, String> jobInfo : list) {
				path = jobInfo.get("path");
				isFile = Integer.parseInt(jobInfo.get("isFile")) == 1;
				size = Long.parseLong(jobInfo.get("size"));
				versionId = jobInfo.get("versionId");
				isDelete = Integer.parseInt(jobInfo.get("isDelete")) == 1;
				isLatest = Integer.parseInt(jobInfo.get("isLatest")) == 1;

				if (isLatest) {
					isDoneLatest = false;
					latestPath = path;
					latestIsFile = isFile;
					latestSize = size;
					latestVersionId = versionId;
					latestIsDelete = isDelete;
					continue;
				}

				if (isNAS) {
					if (isFile) {
						if (size == 0) {
							retryUploadFile(false, path, null);
						} else {
							File file = new File(path);
							retryUploadFile(true, path, file);
						}
					} else {
						retryUploadDirectory(path);
					}
				} else {
					retryMoveObject(path, isDelete, isFile, versionId);
				}
				
				if (isFault) {
					logger.error("move failed : {}", path);
					retryUpdateObjectVersionMoveEventFailed(path, versionId, "", "retry failure");
					retryUpdateJobFailedInfo(size);
				} else {
					retryUpdateJobMoved(size);
					retryUpdateObjectMoveEvent(path, versionId);
				}
			}

			if (!isDoneLatest) {
				if (isNAS) {
					if (latestIsFile) {
						if (latestSize == 0) {
							retryUploadFile(false, latestPath, null);
						} else {
							File file = new File(latestPath);
							retryUploadFile(true, latestPath, file);
						}
					} else {
						retryUploadDirectory(latestPath);
					}
				} else {
					retryMoveObject(latestPath, latestIsDelete, latestIsFile, latestVersionId);
				}
				
				if (isFault) {
					logger.error("move failed : {}", latestPath);
					retryUpdateObjectVersionMoveEventFailed(latestPath, latestVersionId, "", "retry failure");
					retryUpdateJobFailedInfo(latestSize);
				} else {
					retryUpdateJobMoved(latestSize);			
					retryUpdateObjectMoveEvent(latestPath, latestVersionId);
				}
			}
			MDC.remove("logFileName");
		}
	}
}
