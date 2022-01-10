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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.commons.io.FilenameUtils;
import org.json.simple.JSONObject;
import org.openstack4j.api.OSClient;
import org.openstack4j.api.OSClient.OSClientV2;
import org.openstack4j.api.OSClient.OSClientV3;
import org.openstack4j.api.exceptions.AuthenticationException;
import org.openstack4j.api.exceptions.ClientResponseException;
import org.openstack4j.api.exceptions.ConnectionException;
import org.openstack4j.api.exceptions.ResponseException;
import org.openstack4j.api.exceptions.ServerResponseException;
import org.openstack4j.model.common.DLPayload;
import org.openstack4j.model.common.Identifier;
import org.openstack4j.model.common.Payload;
import org.openstack4j.model.common.Payloads;
import org.openstack4j.model.common.header.Range;
import org.openstack4j.model.identity.v2.Access;
import org.openstack4j.model.identity.v3.Token;
import org.openstack4j.model.storage.block.options.DownloadOptions;
import org.openstack4j.model.storage.object.SwiftContainer;
import org.openstack4j.model.storage.object.SwiftObject;
import org.openstack4j.model.storage.object.options.ObjectListOptions;
import org.openstack4j.model.storage.object.options.ObjectLocation;
import org.openstack4j.openstack.OSFactory;

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
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.TagSet;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;

public class ObjectMover {
	private static final Logger logger = LoggerFactory.getLogger(ObjectMover.class);
	
	private Config sourceConfig;
	private Config targetConfig;
	private int threadCount;
	private String type;
	private boolean isSourceAWS;
	private boolean isTargetAWS;
	private boolean isRerun;
	private boolean isVersioning;
	private String jobId;
	private AmazonS3 sourceS3;
	private AmazonS3 targetS3;
	private String pathNAS;
	private BucketVersioningConfiguration sourceS3VersionConfig;

	// for openstack swift
	private Identifier domainIdentifier;
	private Identifier projectIdentifier;
	private OSClientV3 clientV3;

	private static final int RETRY_COUNT = 3;
	private static final String NO_SUCH_KEY = "NoSuchKey";
	private static final String NOT_FOUND = "Not Found";
	private static final long MEGA_BYTES = 1024 * 1024;
	private static final long GIGA_BYTES = 1024 * 1024 * 1024;
	private static final long ONES_MOVE_BYTES = 1024 * 1024 * 500;
	private static final String TYPE_FILE = "file";
	private static final String TYPE_S3 = "s3";
	private static final String TYPE_OS = "swift"; // openstack swift
	private static final String X_STORAGE_POLICY = "X-Storage-Policy";
	private static final String MULTIPART_INFO = "X-Object-Manifest";
	private static final String X_TIMESTAMP = "X-Timestamp";
	private static final String X_OPENSTACK_REQUEST_ID = "X-Openstack-Request-Id";
	private static final String X_TRANS_ID = "X-Trans-Id";
	private static final String X_OBJECT_META_FILE = "X-Object-Meta-File";
	private static final String MTIME = "Mtime";
	private static final String SEGMENTS = "_segments";
	private static final String DEFAULT_ETAG = "d41d8cd98f00b204e9800998ecf8427e";
	private static final CharMatcher VALID_BUCKET_CHAR =
			CharMatcher.inRange('a', 'z')
			.or(CharMatcher.inRange('0', '9'))
			.or(CharMatcher.is('-'))
			.or(CharMatcher.is('.'));

	private static final List<Map<String, String>>movedObjectList = new ArrayList<Map<String, String>>();
	private static final List<Map<String, Long>>movedJobList = new ArrayList<Map<String, Long>>();
	private static final List<Map<String, String>>failedObjectList = new ArrayList<Map<String, String>>();
	private static final List<Map<String, Long>>failedJobList = new ArrayList<Map<String, Long>>();

	private static final String ERROR_TARGET_ENDPOINT_IS_NULL = "target endpoint is null";
	
	public static class Builder {
		private Config sourceConfig;
		private Config targetConfig;
		private int threadCount;
		private String type;
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

		public Builder type(String type) {
			this.type = type;
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
		type = builder.type;
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
		
		if ((targetConfig.getBucket() == null || targetConfig.getBucket().isEmpty()) && type.equalsIgnoreCase(TYPE_S3)) {
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
		
		if (type.equalsIgnoreCase(TYPE_FILE)) {
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
			if (type.compareToIgnoreCase(TYPE_S3) == 0) {
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
			} else if (type.compareToIgnoreCase(TYPE_OS) == 0) {
				String domainId = sourceConfig.getDomainId();
				if (domainId != null && !domainId.isEmpty()) {
					domainIdentifier = Identifier.byId(domainId);
				} else {
					String domainName = sourceConfig.getDomainName();
					if (domainName != null && !domainName.isEmpty()) {
						domainIdentifier = Identifier.byName(domainName);
					} else {
						logger.error("Either domainId or donmainName must be entered.");
						System.out.println("Either domainId or donmainName must be entered.");
						System.exit(-1);
					}
				}

				String projectId = sourceConfig.getProjectId();
				if (projectId != null && !projectId.isEmpty()) {
					projectIdentifier = Identifier.byId(projectId);
				} else {
					String projectName = sourceConfig.getProjectName();
					if (projectName != null && !projectName.isEmpty()) {
						projectIdentifier = Identifier.byName(projectName);
					} else {
						logger.error("Either projectId or projectName must be entered.");
						System.out.println("Either projectId or projectName must be entered.");
						System.exit(-1);
					}
				}

				try {
					clientV3 = OSFactory.builderV3()
						.endpoint(sourceConfig.getAuthEndpoint())
						.credentials(sourceConfig.getUserName(), sourceConfig.getApiKey(), domainIdentifier)
						.scopeToProject(projectIdentifier)
						.authenticate();

					List<? extends SwiftContainer> containers = clientV3.objectStorage().containers().list();
					logger.info("container size : {}", containers.size());
	
					List<String> listContainer = new ArrayList<String>();
					if (sourceConfig.getContainer() != null && !sourceConfig.getContainer().isEmpty()) {
						String[] conContainers = sourceConfig.getContainer().split(",", 0);
						for (int i = 0; i < conContainers.length; i++) {
							listContainer.add(conContainers[i]);
						}
					}

					for (SwiftContainer container : containers) {
						if (container.getName().contains(SEGMENTS)) {
							continue;
						}
						
						if (listContainer.size() > 0) {
							boolean isCheck = false;
							for (String containerName : listContainer) {
								if (container.getName().equals(containerName)) {
									isCheck = true;
									break;
								}
							}
							if (!isCheck) {
								continue;
							}
						} 
						logger.info("container name : {}, object count : {}, object total size : {}", container.getName(), container.getObjectCount(), container.getTotalSize());
						if (!isValidBucketName(container.getName())) {
							String containerName = getS3BucketName(container.getName());
							if (!isValidBucketName(containerName)) {
								logger.error("Error : Container({}) name cannot be changed to S3 bucket name({}).", container.getName(), containerName);
								System.out.println("Error : Container(" + container.getName() + ") name cannot be changed to S3 bucket name(" + containerName + ").");
								System.exit(-1);
							} 
						}
					}
				} catch (AuthenticationException e) {
					if (e.getStatus() == 401) {
						logger.error("user name or api key is invalid.");
						System.out.println("user-name or api-key is invalid.");
					}
					logger.error("Error : {} , {}", e.getStatus(), e.getMessage());
					System.out.println("Error : " + e.getStatus() + " , " + e.getMessage());
					System.exit(-1);
				} catch (ResponseException e) {
					logger.error("Error :r {}, {}", e.getStatus(), e.getMessage());
					System.out.println("Error :r " + e.getStatus() + " , " + e.getMessage());
					System.exit(-1);
				}
			} else {
				System.out.println("undefined type : " + type);
				logger.error("undefined type : {}", type);
				System.exit(-1);
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
		
		if ((targetConfig.getBucket() == null || targetConfig.getBucket().isEmpty()) && type.equalsIgnoreCase(TYPE_S3)) {
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
		
		if (type.equalsIgnoreCase(TYPE_S3) && !existBucket(false, false, targetS3, targetConfig.getBucket())) {
			createBucket(false);
		}

		String prePath = sourceConfig.getMountPoint() + sourceConfig.getPrefix();
		if (!prePath.endsWith("/")) {
			prePath += "/";
		}
		
		if (type.equalsIgnoreCase(TYPE_FILE)) {
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
			if (type.compareToIgnoreCase(TYPE_S3) == 0) {
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
					logger.info("set target-{} versioning is Enabled", targetConfig.getBucket());
				}
				
				if (isRerun) {
					makeObjectVersionListRerun();
				} else {
					makeObjectVersionList();
				}
			} else {
				// openstack swift
				String domainId = sourceConfig.getDomainId();
				if (domainId != null && !domainId.isEmpty()) {
					domainIdentifier = Identifier.byId(domainId);
				} else {
					String domainName = sourceConfig.getDomainName();
					if (domainName != null && !domainName.isEmpty()) {
						domainIdentifier = Identifier.byName(domainName);
					} else {
						logger.error("Either domainId or donmainName must be entered.");
						System.out.println("Either domainId or donmainName must be entered.");
						System.exit(-1);
					}
				}

				String projectId = sourceConfig.getProjectId();
				if (projectId != null && !projectId.isEmpty()) {
					projectIdentifier = Identifier.byId(projectId);
				} else {
					String projectName = sourceConfig.getProjectName();
					if (projectName != null && !projectName.isEmpty()) {
						projectIdentifier = Identifier.byName(projectName);
					} else {
						logger.error("Either projectId or projectName must be entered.");
						System.out.println("Either projectId or projectName must be entered.");
						System.exit(-1);
					}
				}

				List<? extends SwiftContainer> containers = null;
				try {				
					containers = clientV3.objectStorage().containers().list();

					List<String> listContainer = new ArrayList<String>();
					if (sourceConfig.getContainer() != null && !sourceConfig.getContainer().isEmpty()) {
						String[] conContainers = sourceConfig.getContainer().split(",", 0);
						for (int i = 0; i < conContainers.length; i++) {
							listContainer.add(conContainers[i]);
						}
					}

					for (SwiftContainer container : containers) {
						logger.info("container: {}, {}", container.getName(), container.getObjectCount());
						if (container.getName().contains(SEGMENTS)) {
							continue;
						}
						
						if (listContainer.size() > 0) {
							boolean isCheck = false;
							for (String containerName : listContainer) {
								if (container.getName().equals(containerName)) {
									isCheck = true;
									break;
								}
							}
							if (!isCheck) {
								continue;
							}
						} 
						if (!isValidBucketName(container.getName())) {
							String containerName = getS3BucketName(container.getName());
							logger.warn("change S3 Bucket Name : swift : {} -> s3 : {}", container.getName(), containerName);
							if (!isValidBucketName(containerName)) {
								logger.error("Error : Container name cannot be changed to S3 bucket name.");
								System.out.println("Error : Container name cannot be changed to S3 bucket name.");
								System.exit(-1);
							} else {
								createBucket(containerName);
							}
						} else {
							createBucket(container.getName());
						}
					}
				} catch (ResponseException e) {
					logger.error("Error : {}", e.getMessage());
					System.out.println("Error : " + e.getMessage());
					System.exit(-1);
				}

				if (isRerun) {
					makeObjectListSwiftRerun(containers);
				} else {
					makeObjectListSwift(containers);
				}
			}
		}
		
		if (isRerun) {
			DBManager.deleteCheckObjects(jobId);
		}
	}

	private void uploadSwiftObject(OSClientV3 clientV3, String container, String dirPath) {
		File dir = new File(dirPath);
		File[] files = dir.listFiles();
		
		for (int i = 0; i < files.length; i++) {
			if (files[i].isDirectory()) {
				uploadSwiftObject(clientV3, container, files[i].getPath());
			}
			else {
				clientV3.objectStorage().objects().put(container, files[i].getName(), Payloads.create(files[i]));
			}
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
						Mover mover = null;
						if (type.equalsIgnoreCase(TYPE_OS)) {
							mover = new Mover(jobList);
						} else {
							mover = new Mover(jobList);
						}
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
				deleteMove();

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
				}
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}

	private void deleteMove() {
		// delete work
		String finalPath = "";
		String path = "";
		String versionId = "";
		boolean isDoneDistribute = false;
		long sequence = 0;
		long limit = 100;

		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		List<Map<String, String>> list = null;
		List<Map<String, String>> jobList = null;

		sequence = 0;
		isDoneDistribute = false;

		long maxSequence = DBManager.getMaxSequence(jobId);

		while (true) {
			if (!isDoneDistribute) {
				list = DBManager.getToDeleteObjectsInfo(jobId, sequence, limit);
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
				return;
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

	private void createBucket(String bucket) {
		try {
			targetS3.createBucket(bucket);
		} catch (AmazonServiceException ase) {
			if (ase.getErrorCode().compareToIgnoreCase("BucketAlreadyOwnedByYou") == 0) {
				return;
			} else if (ase.getErrorCode().compareToIgnoreCase("BucketAlreadyExists") == 0) {
				return;
			}
			
			logger.error("error code : {}", ase.getErrorCode());
			logger.error(ase.getMessage());
			DBManager.insertErrorJob(jobId, ase.getMessage());
			System.exit(-1);
        }
	}

	private boolean isValidBucketName(String bucketName) {
		if (bucketName == null ||
			bucketName.length() < 3 || bucketName.length() > 63 ||
			bucketName.startsWith(".") || bucketName.startsWith("-") ||
			bucketName.endsWith(".") || bucketName.endsWith("-") || bucketName.endsWith("-s3alias") ||
			!VALID_BUCKET_CHAR.matchesAllOf(bucketName) || 
			bucketName.startsWith("xn--") || bucketName.contains("..") ||
			bucketName.contains(".-") || bucketName.contains("-.")) {
			return false;
		}

		return true;
	}

	private String getS3BucketName(String bucketName) {
		String bucket = bucketName.toLowerCase();
		if (bucket.length() < 3) {
			bucket = "ifs-" + bucket;
		} else if (bucket.length() > 63) {
			bucket = bucket.substring(0, 62);
		}

		if (bucket.startsWith(".") || bucket.startsWith("-")) {
			bucket = bucket.substring(1, bucket.length() - 1);
		}

		if (bucket.startsWith("xn--")) {
			bucket = bucket.substring(4, bucket.length() - 1);
		}

		if (bucket.endsWith(".") || bucket.endsWith("-")) {
			bucket = bucket.substring(0, bucket.length() - 2);
		}

		if (bucket.endsWith("-s3alias")) {
			bucket = bucket.substring(0, bucket.length() - 9);
		}

		bucket = bucket.replace("_", "-");
		bucket = bucket.replace("..", "");
		bucket = bucket.replace(".-", "");
		bucket = bucket.replace("-.", "");
		
		return bucket;
	}

	private void retryInsertMoveObjectVersioning(boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.insertMoveObjectVersioning(jobId, isFile, mTime, size, path, versionId, etag, multipartInfo, tag, isDelete, isLatest)) {
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

	private void retryInsertRerunMoveObjectVersion(boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (DBManager.insertRerunMoveObjectVersion(jobId, isFile, mTime, size, path, versionId, etag, multipartInfo, tag, isDelete, isLatest)) {
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
							versionSummary.getETag(),
							"",
							"",
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
						Map<String, String> info = DBManager.infoExistObjectVersion(jobId, versionSummary.getKey(), versionSummary.getVersionId());
						if (info.isEmpty()) {
							retryInsertRerunMoveObjectVersion(versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/', 
								versionSummary.getLastModified().toString(), 
								versionSummary.getSize(), 
								versionSummary.getKey(), 
								versionSummary.getVersionId(), 
								versionSummary.getETag(),
								"",
								"",
								versionSummary.isDeleteMarker(), 
								versionSummary.isLatest());
							retryUpdateJobRerunInfo(versionSummary.getSize());
						} else {
							state = Integer.parseInt(info.get("object_state"));
							String mTime = info.get("mtime");
							if (state == 3 && mTime.compareTo(versionSummary.getLastModified().toString()) == 0) {	
								retryUpdateRerunSkipObjectVersion(versionSummary.getKey(), versionSummary.getVersionId());
								retryUpdateJobRerunSkipInfo(versionSummary.getSize());
							} else {
								retryUpdateToMoveObjectVersion(versionSummary.getLastModified().toString(), versionSummary.getSize(), versionSummary.getKey(), versionSummary.getVersionId());
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

	private void makeObjectListSwift(List<? extends SwiftContainer> containers) {
		long count = 0;
		try  {
			OSClientV3 V3 = OSFactory.builderV3()
				.endpoint(sourceConfig.getAuthEndpoint())
				.credentials(sourceConfig.getUserName(), sourceConfig.getApiKey(), domainIdentifier)
				.scopeToProject(projectIdentifier)
				.authenticate();

			List<? extends SwiftObject> objs = null;
			List<String> listContainer = new ArrayList<String>();
			if (sourceConfig.getContainer() != null && !sourceConfig.getContainer().isEmpty()) {
				String[] configContainers = sourceConfig.getContainer().split(",", 0);
				for (int i = 0; i < configContainers.length; i++) {
					listContainer.add(configContainers[i]);
				}
			}

			for (SwiftContainer container : containers) {
				if (container.getName().contains(SEGMENTS)) {
					continue;
				}
				
				if (listContainer.size() > 0) {
					boolean isCheck = false;
					for (String containerName : listContainer) {
						if (container.getName().equals(containerName)) {
							isCheck = true;
							break;
						}
					}
					if (!isCheck) {
						continue;
					}
				}

				Map<String, String> containerMap = container.getMetadata();
				List<TagSet> tags = new ArrayList<TagSet>();
				for (String key : containerMap.keySet()) {
					// if (key.equalsIgnoreCase(MULTIPART_INFO)
					// 	|| key.equalsIgnoreCase(X_TIMESTAMP)
					// 	|| key.equalsIgnoreCase(X_OPENSTACK_REQUEST_ID)
					// 	|| key.equalsIgnoreCase(X_TRANS_ID)
					// 	|| key.equalsIgnoreCase(MTIME)) {
					// 	continue;
					// }
					TagSet set = new TagSet();
					set.setTag(key, containerMap.get(key));
					tags.add(set);
				}
				if (tags.size() > 0) {
					String bucketName = container.getName();
					if (!isValidBucketName(bucketName)) {
						bucketName = getS3BucketName(bucketName);
						if (!isValidBucketName(bucketName)) {
							logger.error("Error : Container name cannot be changed to S3 bucket name.");
							System.out.println("Error : Container name cannot be changed to S3 bucket name.");
							System.exit(-1);
						}
					}
					BucketTaggingConfiguration configuration = new BucketTaggingConfiguration();
					configuration.setTagSets(tags);
					targetS3.setBucketTaggingConfiguration(bucketName, configuration);
				}

				ObjectListOptions listOptions = ObjectListOptions.create().limit(10000);
				if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
					listOptions.startsWith(sourceConfig.getPrefix());
				}
				
				do {
					objs = clientV3.objectStorage().objects().list(container.getName(), listOptions);
					if (!objs.isEmpty()) {
						listOptions.marker(objs.get(objs.size() - 1).getName());
					}
					
					for (SwiftObject obj : objs) {
						Map<String, String> meta = obj.getMetadata();
						long size = obj.getSizeInBytes();
	
						JSONObject json = new JSONObject();
						for (String key : meta.keySet()) {
							if (key.equalsIgnoreCase(MULTIPART_INFO)
								|| key.equalsIgnoreCase(X_TIMESTAMP)
								|| key.equalsIgnoreCase(X_OPENSTACK_REQUEST_ID)
								|| key.equalsIgnoreCase(X_TRANS_ID)
								|| key.equalsIgnoreCase(MTIME)
								|| key.equalsIgnoreCase(X_OBJECT_META_FILE)) {
								continue;
							}
							json.put(key, meta.get(key));
						}
						
						
						if (meta.get(MULTIPART_INFO) != null) {
							String[] path = meta.get(MULTIPART_INFO).split("/", 2);
							int i = 0;
							SwiftObject object = null;
							do {
								String partPath = String.format("%08d", i++);
								partPath = path[1] + partPath;
								
								object = V3.objectStorage().objects().get(path[0], partPath);
								if (object != null) {
									size += object.getSizeInBytes();
								}
							} while (object != null);
						}
						count++;
						retryInsertMoveObjectVersioning(!obj.isDirectory(),  
							obj.getLastModified().toString(),
							size,
							container.getName() + "/" + obj.getName(), 
							"",
							obj.getETag(),
							meta.get(MULTIPART_INFO),
							json.toString(),
							false,
							true);
						retryUpdateJobInfo(size);
						// OSClient v3 = OSFactory.builderV3()
						// 	.endpoint(sourceConfig.getAuthEndpoint())
						// 	.credentials(sourceConfig.getUserName(), sourceConfig.getApiKey(), domainIdentifier)
						// 	.scopeToProject(projectIdentifier)
						// 	.authenticate();
						// Map<String, String> map = new HashMap<String, String>();
						// map.put("uuid", UUID.randomUUID().toString());
						// v3.objectStorage().objects().updateMetadata(ObjectLocation.create(container.getName(), obj.getName()), map);
					}
				} while (objs.size() >= 10000);
			}

			if (count == 0) {
				logger.error("Doesn't havn an object.");
				DBManager.insertErrorJob(jobId, "Doesn't havn an object.");
				System.exit(-1);
			}
		} catch (ResponseException e) {
			logger.error("Error code : {}", e.getStatus());
			logger.error("Error message : {}", e.getMessage());
			DBManager.insertErrorJob(jobId, e.getStatus() + "," + e.getMessage());
			System.exit(-1);
		}
	}

	private void makeObjectListSwiftRerun(List<? extends SwiftContainer> containers) {
		long state = 0;
		try  {
			List<? extends SwiftObject> objs = null;	
			OSClientV3 V3 = OSFactory.builderV3()
				.endpoint(sourceConfig.getAuthEndpoint())
				.credentials(sourceConfig.getUserName(), sourceConfig.getApiKey(), domainIdentifier)
				.scopeToProject(projectIdentifier)
				.authenticate();

			List<String> listContainer = new ArrayList<String>();
			if (sourceConfig.getContainer() != null && !sourceConfig.getContainer().isEmpty()) {
				String[] configContainers = sourceConfig.getContainer().split(",", 0);
				for (int i = 0; i < configContainers.length; i++) {
					listContainer.add(configContainers[i]);
				}
			}

			for (SwiftContainer container : containers) {
				if (container.getName().contains(SEGMENTS)) {
					continue;
				}
				
				if (listContainer.size() > 0) {
					boolean isCheck = false;
					for (String containerName : listContainer) {
						if (container.getName().equals(containerName)) {
							isCheck = true;
							break;
						}
					}
					if (!isCheck) {
						continue;
					}
				}

				Map<String, String> containerMap = container.getMetadata();
				List<TagSet> tags = new ArrayList<TagSet>();
				for (String key : containerMap.keySet()) {
					// if (key.equalsIgnoreCase(MULTIPART_INFO)
					// 	|| key.equalsIgnoreCase(X_TIMESTAMP)
					// 	|| key.equalsIgnoreCase(X_OPENSTACK_REQUEST_ID)
					// 	|| key.equalsIgnoreCase(X_TRANS_ID)
					// 	|| key.equalsIgnoreCase(MTIME)) {
					// 	continue;
					// }
					TagSet set = new TagSet();
					set.setTag(key, containerMap.get(key));
					tags.add(set);
				}
				if (tags.size() > 0) {
					String bucketName = container.getName();
					if (!isValidBucketName(bucketName)) {
						bucketName = getS3BucketName(bucketName);
						if (!isValidBucketName(bucketName)) {
							logger.error("Error : Container name cannot be changed to S3 bucket name.");
							System.out.println("Error : Container name cannot be changed to S3 bucket name.");
							System.exit(-1);
						}
					}
					BucketTaggingConfiguration configuration = new BucketTaggingConfiguration();
					configuration.setTagSets(tags);
					targetS3.setBucketTaggingConfiguration(bucketName, configuration);
				}

				ObjectListOptions listOptions = ObjectListOptions.create().limit(10000);
				if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
					listOptions.startsWith(sourceConfig.getPrefix());
				}

				do {
					objs = clientV3.objectStorage().objects().list(sourceConfig.getContainer(), listOptions);
					if (!objs.isEmpty()) {
						listOptions.marker(objs.get(objs.size() - 1).getName());
					}
					
					for (SwiftObject obj : objs) {
						Map<String, String> meta = obj.getMetadata();
						long size = obj.getSizeInBytes();
	
						JSONObject json = new JSONObject();
						for (String key : meta.keySet()) {
							if (key.equalsIgnoreCase(MULTIPART_INFO)
								|| key.equalsIgnoreCase(X_TIMESTAMP)
								|| key.equalsIgnoreCase(X_OPENSTACK_REQUEST_ID)
								|| key.equalsIgnoreCase(X_TRANS_ID)
								|| key.equalsIgnoreCase(MTIME)
								|| key.equalsIgnoreCase(X_OBJECT_META_FILE)) {
								continue;
							}
							json.put(key, meta.get(key));
						}
						
						if (meta.get(MULTIPART_INFO) != null) {
							String[] path = meta.get(MULTIPART_INFO).split("/", 2);
							int i = 0;
							SwiftObject object = null;
							do {
								String partPath = String.format("%08d", i++);
								partPath = path[1] + partPath;
								
								object = V3.objectStorage().objects().get(path[0], partPath);
								if (object != null) {
									size += object.getSizeInBytes();
								}
							} while (object != null);
						}

						Map<String, String> info = DBManager.infoExistObject(jobId, obj.getName());
						if (info.isEmpty()) {
							retryInsertMoveObjectVersioning(!obj.isDirectory(), 
								obj.getLastModified().toString(),
								obj.getSizeInBytes(),
								obj.getName(), 
								"",
								obj.getETag(),
								meta.get(MULTIPART_INFO),
								json.toString(),
								false,
								true);
							retryUpdateJobInfo(obj.getSizeInBytes());
						} else {
							state = Integer.parseInt(info.get("object_state"));
							String mTime = info.get("mtime");
							if (state == 3 && mTime.compareTo(obj.getLastModified().toString()) == 0) {	
								retryUpdateRerunSkipObjectVersion(obj.getName(), "");
								retryUpdateJobRerunSkipInfo(obj.getSizeInBytes());
							} else {
								retryUpdateToMoveObjectVersion(obj.getLastModified().toString(), obj.getSizeInBytes(), obj.getName(), "");
								retryUpdateJobRerunInfo(obj.getSizeInBytes());
							}
						}
					}
				} while (objs.size() >= 10000);
			}
		} catch (ResponseException e) {
			logger.error("Error code : {}", e.getStatus());
			logger.error("Error message : {}", e.getMessage());
			DBManager.insertErrorJob(jobId, e.getStatus() + "," + e.getMessage());
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
		private long moveSize;

		private String path;
		private String latestPath;
		private long size;
		private long latestSize;
		private boolean isFile;
		private boolean latestIsFile;
		private String versionId;
		private String latestVersionId;
		private String etag;
		private String latestETag;
		private String multipartInfo;
		private String latestMultipartInfo;
		private String tag;
		private String latestTag;
		private boolean isDelete;
		private boolean latestIsDelete;
		private boolean isLatest;

		private boolean isFault;
		private boolean isDoneLatest;

		private OSClientV3 clientV3;
		
		public Mover(List<Map<String, String>> list) {
			this.list = list;
			getMoveSize();
		}

		Mover(String path, long size, boolean isFile, String versionId, String etag, boolean isDelete) {
			this.path = path;
			this.size = size;
			this.isFile = isFile;
			this.versionId = versionId;
			this.etag = etag;
			this.isDelete = isDelete;
		}

		private void getMoveSize() {
			String confMoveSize = sourceConfig.getMoveSize();
			if (confMoveSize == null || confMoveSize.length() == 0) {
				moveSize = 0L;
			} else {
				moveSize = Integer.parseInt(sourceConfig.getMoveSize()) * MEGA_BYTES;
			}
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
		
		private boolean moveObject(String path, boolean isDelete, boolean isFile, String versionId, String etag, String multipartInfo, String tag, long size) {
			String sourcePath = path;
			String targetObject = path;
			String tETag = null;
			Map<String, String> tagMap = null;
			List<Tag> tagSet = new ArrayList<Tag>();
			
			if (sourceConfig.getPrefix() != null && !sourceConfig.getPrefix().isEmpty()) {
				path = path.substring(sourceConfig.getPrefix().length());
				if (path.startsWith("/")) {
					path = path.substring(1);
				}
			}
			
			if (type.equalsIgnoreCase(TYPE_OS)) {
				targetObject = path.split("/", 2)[1];
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
					if (type.equalsIgnoreCase(TYPE_OS)) {
						targetObject = prefixFinal + targetObject;
					} else {
						targetObject = prefixFinal + path;
					}
				}
			}

			if (tag != null && !tag.isEmpty()) {
				try {
					tagMap = new ObjectMapper().readValue(tag, Map.class);
					for (String key : tagMap.keySet()) {
						Tag s3tag = new Tag(new String(key.getBytes("UTF-8"), Charset.forName("UTF-8")), new String(tagMap.get(key).getBytes("UTF-8"), Charset.forName("UTF-8")));
						tagSet.add(s3tag);
					}
				} catch (JsonParseException e) {
					logger.error(path + "," + e.getMessage());
					tagMap = null;
				} catch (JsonMappingException e) {
					logger.error(path + "," + e.getMessage());
					tagMap = null;
				} catch (IOException e) {
					logger.error(path + "," + e.getMessage());
					tagMap = null;
				}
			}

			if (type.compareToIgnoreCase(TYPE_S3) == 0) {
				try {
					if (isDelete) {
						targetS3.deleteObject(targetConfig.getBucket(), targetObject);
						logger.info("delete success : {}", sourcePath);
					} else {
						InputStream is = null;
						ObjectMetadata meta = null;
						GetObjectRequest getObjectRequest = null;
						S3Object objectData = null;
	
						if (isFile) {
							if (moveSize != 0 && size > moveSize) {
								InitiateMultipartUploadResult initMultipart = targetS3.initiateMultipartUpload(new InitiateMultipartUploadRequest(targetConfig.getBucket(), targetObject));
								String uploadId = initMultipart.getUploadId();
								List<PartETag> partList = new ArrayList<PartETag>();
								int partNumber = 1;
								for (long i = 0; i < size; i += moveSize, partNumber++) {
									long start = i;
									long end = i + moveSize - 1;
									if (end >= size) {
										end = size - 1;
									}
	
									if (versionId == null) {
										getObjectRequest = new GetObjectRequest(sourceConfig.getBucket(), sourcePath).withRange(start, end);
									} else {
										getObjectRequest = new GetObjectRequest(sourceConfig.getBucket(), sourcePath).withVersionId(versionId).withRange(start, end);
									}
									objectData = sourceS3.getObject(getObjectRequest);
									is = objectData.getObjectContent();
									
									UploadPartResult partResult = targetS3.uploadPart(new UploadPartRequest().withBucketName(targetConfig.getBucket()).withKey(targetObject)
											.withUploadId(uploadId).withInputStream(is).withPartNumber(partNumber).withPartSize(end - start + 1));
									partList.add(new PartETag(partNumber, partResult.getETag()));
									is.close();
								}

								CompleteMultipartUploadResult result = targetS3.completeMultipartUpload(new CompleteMultipartUploadRequest(targetConfig.getBucket(), targetObject, uploadId, partList));
								tETag = result.getETag();
								if (etag.equals(tETag)) {
									if (versionId != null && !versionId.isEmpty()) {
										logger.info("move success : {}:{}", sourcePath, versionId);
									} else {
										logger.info("move success : {}", sourcePath);
									}
								} else {
									logger.warn("The etags are different. source : {}, target : {}", etag, tETag);
									return false;
								}
							} else {
								if (versionId == null) {
									getObjectRequest = new GetObjectRequest(sourceConfig.getBucket(), sourcePath);
								} else {
									getObjectRequest = new GetObjectRequest(sourceConfig.getBucket(), sourcePath).withVersionId(versionId);
								}
								objectData = sourceS3.getObject(getObjectRequest);
								is = objectData.getObjectContent();
								meta = objectData.getObjectMetadata();
	
								PutObjectRequest putObjectRequest;
								putObjectRequest = new PutObjectRequest(targetConfig.getBucket(), targetObject, is, meta);
								putObjectRequest.getRequestClientOptions().setReadLimit(1024 * 1024 * 1024);
								PutObjectResult result = targetS3.putObject(putObjectRequest);
								tETag = result.getETag();
								if (etag.equals(tETag)) {
									if (versionId != null && !versionId.isEmpty()) {
										logger.info("move success : {}:{}", sourcePath, versionId);
									} else {
										logger.info("move success : {}", sourcePath);
									}
								} else {
									logger.warn("The etags are different. source : {}, target : {}", etag, tETag);
									return false;
								}
							}
						} else {
							meta = new ObjectMetadata();
							meta.setContentLength(0);
							is = new ByteArrayInputStream(new byte[0]);
	
							PutObjectRequest putObjectRequest;
							putObjectRequest = new PutObjectRequest(targetConfig.getBucket(), targetObject, is, meta);
							targetS3.putObject(putObjectRequest);
							if (versionId != null && !versionId.isEmpty()) {
								logger.info("move success : {}:{}", sourcePath, versionId);
							} else {
								logger.info("move success : {}", sourcePath);
							}
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
					logger.warn("{}", sourcePath, ace);
					return false;
				} catch (IOException e) {
					logger.warn("{} {}", sourcePath, e.getMessage());
					return false;
				}
			} else {
				// openstack swift
				try {
					InputStream is = null;
					ObjectMetadata meta = null;
					
					String[] sourcePaths = sourcePath.split("/", 2);
					String sourceBucket = sourcePaths[0];
					String sourceObject = sourcePaths[1];
					String targetBucket = sourceBucket;
					if (!isValidBucketName(targetBucket)) {
						targetBucket = getS3BucketName(targetBucket);
					}

					if (isFile) {
						clientV3 = OSFactory.builderV3()
							.endpoint(sourceConfig.getAuthEndpoint())
							.credentials(sourceConfig.getUserName(), sourceConfig.getApiKey(), domainIdentifier)
							.scopeToProject(projectIdentifier)
							.authenticate();

						if (multipartInfo != null) {
							InitiateMultipartUploadResult initMultipart = targetS3.initiateMultipartUpload(new InitiateMultipartUploadRequest(targetBucket, targetObject));
							String uploadId = initMultipart.getUploadId();
							List<PartETag> partList = new ArrayList<PartETag>();
							String[] multiPath = multipartInfo.split("/", 2);
							int partNumber = 0;
							SwiftObject object = null;
							do {
								String partPath = String.format("%08d", partNumber++);
								partPath = multiPath[1] + partPath;
								object = clientV3.objectStorage().objects().get(multiPath[0], partPath);

								if (object != null) {
									DLPayload load = object.download();
									is = load.getInputStream();

									UploadPartResult partResult = targetS3.uploadPart(new UploadPartRequest().withBucketName(targetBucket).withKey(targetObject)
											.withUploadId(uploadId).withInputStream(is).withPartNumber(partNumber).withPartSize(object.getSizeInBytes()));
									partList.add(new PartETag(partNumber, partResult.getETag()));
									is.close();
								}
							} while (object != null);
							tETag = targetS3.completeMultipartUpload(new CompleteMultipartUploadRequest(targetBucket, targetObject, uploadId, partList)).getETag();
							if (tagSet.size() > 0) {
								targetS3.setObjectTagging(new SetObjectTaggingRequest(targetBucket, targetObject, new ObjectTagging(tagSet)));
							}
							logger.info("move success : {}", sourcePath);
						} else if (moveSize != 0 && size > moveSize) {
							try {
								InitiateMultipartUploadResult initMultipart = targetS3.initiateMultipartUpload(new InitiateMultipartUploadRequest(targetBucket, targetObject));
								String uploadId = initMultipart.getUploadId();
								List<PartETag> partList = new ArrayList<PartETag>();
								int partNumber = 1;
								for (long i = 0; i < size; i += moveSize, partNumber++) {
									long start = i;
									long end = i + moveSize - 1;
									if (end >= size) {
										end = size - 1;
									}
									DownloadOptions options = DownloadOptions.create();
									options.range(Range.from(start, end));
									DLPayload load = clientV3.objectStorage().objects().get(sourceBucket, sourceObject).download(options);
									is = load.getInputStream();
									UploadPartResult partResult = targetS3.uploadPart(new UploadPartRequest().withBucketName(targetBucket).withKey(targetObject)
											.withUploadId(uploadId).withInputStream(is).withPartNumber(partNumber).withPartSize(end - start + 1));
									partList.add(new PartETag(partNumber, partResult.getETag()));
									is.close();
								}

								tETag = targetS3.completeMultipartUpload(new CompleteMultipartUploadRequest(targetBucket, targetObject, uploadId, partList)).getETag();
								if (tagSet.size() > 0) {
									targetS3.setObjectTagging(new SetObjectTaggingRequest(targetBucket, targetObject, new ObjectTagging(tagSet)));
								}
								logger.info("move success : {}", sourcePath);
							} catch (Exception e) {
								logger.error(e.getMessage());
							}
						} else {
							if (size > GIGA_BYTES) {
								InitiateMultipartUploadResult initMultipart = targetS3.initiateMultipartUpload(new InitiateMultipartUploadRequest(targetBucket, targetObject));
								String uploadId = initMultipart.getUploadId();
								List<PartETag> partList = new ArrayList<PartETag>();
								int partNumber = 1;
								for (long i = 0; i < size; i += ONES_MOVE_BYTES, partNumber++) {
									long start = i;
									long end = i + ONES_MOVE_BYTES - 1;
									if (end >= size) {
										end = size - 1;
									}

									DownloadOptions options = DownloadOptions.create();
									options.range(Range.from(start, end));
									DLPayload load = clientV3.objectStorage().objects().get(sourceBucket, sourceObject).download(options);
									is = load.getInputStream();
									UploadPartResult partResult = targetS3.uploadPart(new UploadPartRequest().withBucketName(targetBucket).withKey(targetObject)
											.withUploadId(uploadId).withInputStream(is).withPartNumber(partNumber).withPartSize(end - start + 1));
									partList.add(new PartETag(partNumber, partResult.getETag()));
									is.close();
								}

								tETag = targetS3.completeMultipartUpload(new CompleteMultipartUploadRequest(targetBucket, targetObject, uploadId, partList)).getETag();
								if (tagSet.size() > 0) {
									targetS3.setObjectTagging(new SetObjectTaggingRequest(targetBucket, targetObject, new ObjectTagging(tagSet)));
								}
								logger.info("move success : {}", sourcePath);
							} else {
								DLPayload load = clientV3.objectStorage().objects().get(sourceBucket, sourceObject).download();
								is = load.getInputStream();
								PutObjectRequest putObjectRequest;
								meta = new ObjectMetadata();
								meta.setContentLength(size);
								putObjectRequest = new PutObjectRequest(targetBucket, targetObject, is, meta);
								putObjectRequest.getRequestClientOptions().setReadLimit(1024 * 1024 * 1024);
								PutObjectResult result = targetS3.putObject(putObjectRequest);
								tETag = result.getETag();
								if (tagSet.size() > 0) {
									targetS3.setObjectTagging(new SetObjectTaggingRequest(targetBucket, targetObject, new ObjectTagging(tagSet)));
								}
								if (etag.equals(tETag)) {
									logger.info("move success : {}", sourcePath);
								} else {
									logger.warn("The etags are different. source : {}, target : {}", etag, tETag);
									return false;
								}
							}
						}
					} else {
						targetObject += "/";
						meta = new ObjectMetadata();
						meta.setContentLength(0);
						is = new ByteArrayInputStream(new byte[0]);
						PutObjectRequest putObjectRequest;
						putObjectRequest = new PutObjectRequest(targetBucket, targetObject, is, meta);
						putObjectRequest.getRequestClientOptions().setReadLimit(1024 * 1024 * 1024);
						targetS3.putObject(putObjectRequest);
						if (tagSet.size() > 0) {
							targetS3.setObjectTagging(new SetObjectTaggingRequest(targetBucket, targetObject, new ObjectTagging(tagSet)));
						}
						logger.info("move success : {}", sourcePath);
					}
				} catch (AmazonServiceException ase) {
					if (ase.getErrorCode().compareToIgnoreCase(NO_SUCH_KEY) == 0) {
						logger.warn("{} {}", sourcePath, ase.getErrorMessage());
					} else if (ase.getErrorMessage().contains(NOT_FOUND)) {
						logger.warn("{} {}", sourcePath, ase.getErrorMessage());
					} else {
						logger.warn("{} {} - {}", sourcePath, ase.getErrorCode(), ase.getErrorMessage());
					}
					return false;
				} catch (AmazonClientException ace) {
					logger.warn("{} {}", sourcePath, ace);
					return false;
				} catch (Exception e) {
					logger.error(e.toString());
				}
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
		
		private void retryMoveObject(String path, boolean isDelete, boolean isFile, String versionId, String etag, String multipartInfo, String tag, long size) {
			for (int i = 0; i < RETRY_COUNT; i++) {
				if (moveObject(path, isDelete, isFile, versionId, etag, multipartInfo, tag, size)) {
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
				etag = jobInfo.get("etag");
				multipartInfo = jobInfo.get("multipart_info");
				tag = jobInfo.get("tag");
				isDelete = Integer.parseInt(jobInfo.get("isDelete")) == 1;
				isLatest = Integer.parseInt(jobInfo.get("isLatest")) == 1;

				if (isLatest) {
					isDoneLatest = false;
					latestPath = path;
					latestIsFile = isFile;
					latestSize = size;
					latestVersionId = versionId;
					latestETag = etag;
					latestMultipartInfo = multipartInfo;
					latestTag = tag;
					latestIsDelete = isDelete;
					continue;
				}

				if (type.equalsIgnoreCase(TYPE_FILE)) {
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
					retryMoveObject(path, isDelete, isFile, versionId, etag, multipartInfo, tag, size);
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
				if (type.equalsIgnoreCase(TYPE_FILE)) {
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
					retryMoveObject(latestPath, latestIsDelete, latestIsFile, latestVersionId, latestETag, latestMultipartInfo, latestTag, size);
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
