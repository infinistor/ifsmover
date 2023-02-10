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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GetObjectAclRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
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
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;

import ifs_mover.Config;
import ifs_mover.SyncMode;
import ifs_mover.Utils;
import ifs_mover.db.MariaDB;

public class IfsS3 implements Repository, S3 {
	private static final Logger logger = LoggerFactory.getLogger(IfsS3.class);
	private String jobId;
	private boolean isSource;
    private Config  config;
    private boolean isAWS;
    private boolean isSecure;
	private boolean isVersioning;
	private boolean isTargetSync;
	private SyncMode targetSyncMode;
	private String versioningStatus;
    private AmazonS3 client;
	private String errCode;
	private String errMessage;
	private BucketVersioningConfiguration versionConfig;
	private boolean targetVersioning;

	private final String HTTPS = "https";
	private final String AWS_S3_V4_SIGNER_TYPE = "AWSS3V4SignerType";
	private final String BUCKET_ALREADY_OWNED_BY_YOU = "BucketAlreadyOwnedByYou";
	private final String BUCKET_ALREADY_EXISTS = "BucketAlreadyExists";
	private final String INVALID_ACCESS_KEY_ID = "InvalidAccessKeyId";
	private final String SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch";
	private final String ACCESS_DENIED = "AccessDenied";

	private final int MILLISECONDS = 1000;
	private final int TIMEOUT = 300;
	private final int RETRY_COUNT = 2;

	private final String LOG_SOURCE_INVALID_ACCESS = "source - The access key is invalid.";
	private final String LOG_SOURCE_INVALID_SECRET = "source - The secret key is invalid.";
	private final String LOG_TARGET_INVALID_ACCESS = "target - The access key is invalid.";
	private final String LOG_TARGET_INVALID_SECRET = "target - The secret key is invalid.";
	private final String LOG_SOURCE_ENDPOINT_NULL = "source - endpoint is null";
	private final String LOG_TARGET_ENDPOINT_NULL = "target - endpoint is null";
	private final String LOG_SOURCE_BUCKET_NULL = "source - bucket is null";
	private final String LOG_TARGET_BUCKET_NULL = "target - bucket is null";
	private final String LOG_SOURCE_BUCKET_NOT_EXIST = "source - bucket is not exist";
	private final String LOG_SOURCE_NOT_REGION = "source - unable to find region.";
	private final String LOG_TARGET_NOT_REGION = "target - unable to find region.";
	private final String LOG_SOURCE_INVALID_ENDPOINT = "source - endpoint is invalid.";
	private final String LOG_TARGET_INVALID_ENDPOINT = "target - endpoint is invalid.";
	private final String LOG_SOURCE_ACCESS_DENIED = "source - Bucket exists, but does not have access.";
	private final String LOG_TARGET_ACCESS_DENIED = "target - Bucket exists, but does not have access.";

	IfsS3(String jobId) {
		this.jobId = jobId;
	}

    @Override
    public void setConfig(Config config, boolean isSource) {
        this.config = config;
		this.isSource = isSource;
        isAWS = config.isAWS();
        isSecure = isAWS;
		if (!isSource) {
			isTargetSync = config.isTargetSync();
			targetSyncMode = config.getSyncMode();
		} else {
			isTargetSync = false;
			targetSyncMode = SyncMode.UNKNOWN;
		}
    }

    @Override
    public int check(String type) {
		if (config.getEndPoint() == null || config.getEndPoint().isEmpty()) {
			if (isSource) {
				logger.error(LOG_SOURCE_ENDPOINT_NULL);
				errMessage = LOG_SOURCE_ENDPOINT_NULL;
			} else {
				logger.error(LOG_TARGET_ENDPOINT_NULL);
				errMessage = LOG_TARGET_ENDPOINT_NULL;
			}
			return ENDPOINT_IS_NULL;
		}
		
		if (!type.equalsIgnoreCase(Repository.SWIFT)) {
			if (config.getBucket() == null || config.getBucket().isEmpty()) {
				if (isSource) {
					logger.error(LOG_SOURCE_BUCKET_NULL);
					errMessage = LOG_SOURCE_BUCKET_NULL;
				} else {
					logger.error(LOG_TARGET_BUCKET_NULL);
					errMessage = LOG_TARGET_BUCKET_NULL;
				}
				return BUCKET_IS_NULL;
			}
		}

		if (isAWS) {
			isSecure = true;
		} else {
			if (config.getEndPointProtocol().compareToIgnoreCase(HTTPS) == 0) {
				isSecure = true;
			} else {
				isSecure = false;
			}
		}

        int result = getClient();
		if (result != NO_ERROR) {
			return result;
		}

		result = existBucket(true, config.getBucket());
		if (result == BUCKET_NO_EXIST) {
			if (isSource) {
				logger.error(LOG_SOURCE_BUCKET_NOT_EXIST);
				errMessage = LOG_SOURCE_BUCKET_NOT_EXIST;
				return BUCKET_NO_EXIST;
			} else {
				result = createBucket(true);
				if (result != NO_ERROR) {
					return result;
				}
			}
		} else if (result != NO_ERROR) {
			return result;
		}

        return NO_ERROR;
    }

    public int getClient() {
        try {
			client = createClient(isAWS, isSecure, config.getEndPoint(), config.getAccessKey(), config.getSecretKey());
		} catch (SdkClientException e) {
			if (isSource) {
				logger.error(LOG_SOURCE_NOT_REGION);
				errMessage = LOG_SOURCE_NOT_REGION;
			} else {
				logger.error(LOG_TARGET_NOT_REGION);
				errMessage = LOG_TARGET_NOT_REGION;
			}
			
            return UNABLE_FIND_REGION;
		} catch (IllegalArgumentException e) {
			if (isSource) {
				logger.error(LOG_SOURCE_INVALID_ENDPOINT);
				errMessage = LOG_SOURCE_INVALID_ENDPOINT;
			} else {
				logger.error(LOG_TARGET_INVALID_ENDPOINT);
				errMessage = LOG_TARGET_INVALID_ENDPOINT;
			}
            return INVALID_ENDPOINT;
		}

        return NO_ERROR;
    }

    private AmazonS3 createClient(boolean isAWS, boolean isSecure, String URL, String AccessKey, String SecretKey) throws SdkClientException, IllegalArgumentException{
		ClientConfiguration config;

		if (isSecure) {
			config = new ClientConfiguration().withProtocol(Protocol.HTTPS);
		} else {
			config = new ClientConfiguration().withProtocol(Protocol.HTTP);
		}

		config.setSignerOverride(AWS_S3_V4_SIGNER_TYPE);
		config.setMaxErrorRetry(RETRY_COUNT);
		config.setConnectionTimeout(TIMEOUT * MILLISECONDS);
		config.setSocketTimeout(TIMEOUT * MILLISECONDS);
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

    private int existBucket(boolean isCheck, String bucket) {
		int result = 0;
		try {
			if (client.doesBucketExistV2(bucket)) {
				ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withMaxKeys(10);
				ObjectListing list = client.listObjects(request);
				result = NO_ERROR;
			} else {
				result = BUCKET_NO_EXIST;
			}
		} catch (AmazonServiceException ase) {
			errCode = ase.getErrorCode();
			switch (errCode) {
			case INVALID_ACCESS_KEY_ID:
				if (isSource) {
					logger.error(LOG_SOURCE_INVALID_ACCESS);
					errMessage = LOG_SOURCE_INVALID_ACCESS;
				} else {
					logger.error(LOG_TARGET_INVALID_ACCESS);
					errMessage = LOG_TARGET_INVALID_ACCESS;
				}
				result = INVALID_ACCESS_KEY;
				break;
				
			case SIGNATURE_DOES_NOT_MATCH:
				if (isSource) {
					logger.error(LOG_SOURCE_INVALID_SECRET);
					errMessage = LOG_SOURCE_INVALID_SECRET;
				} else {
					logger.error(LOG_TARGET_INVALID_SECRET);
					errMessage = LOG_TARGET_INVALID_SECRET;
				}
				result = INVALID_SECRET_KEY;
				break;

			case ACCESS_DENIED:
				if (isSource) {
					logger.error(LOG_SOURCE_ACCESS_DENIED);
					errMessage = LOG_SOURCE_ACCESS_DENIED;
				} else {
					logger.error(LOG_TARGET_ACCESS_DENIED);
					errMessage = LOG_TARGET_ACCESS_DENIED;
				}
				result = ACCESS_DENIED_ERROR;
				break;

			default:
				if (isSource) {
					logger.error("source - " + errCode);
					errMessage = "source - " + errCode;
				} else {
					logger.error("target - " + errCode);
					errMessage = "target - " + errCode;
				}
				result = AMAZON_SERVICE_EXCEPTION;
				break;
			}
        } catch (AmazonClientException ace) {
        	if (isCheck) {
        		if (isSource) {
					logger.error("source - {}", ace.getMessage());
					errMessage = "source - " + ace.getMessage();
        		} else {
					logger.error("target - {}", ace.getMessage());
					errMessage = "target - " + ace.getMessage();
        		}
			}
			result = AMAZON_CLIENT_EXCEPTION;
        }
		
		return result;
	}

	private int createBucket(boolean isCheck) {
		try {
			client.createBucket(config.getBucket());
			if (isCheck) {
				client.deleteBucket(config.getBucket());
			}
			return NO_ERROR;
		} catch (AmazonServiceException ase) {
			if (ase.getErrorCode().compareToIgnoreCase(BUCKET_ALREADY_OWNED_BY_YOU) == 0) {
				return NO_ERROR;
			} else if (ase.getErrorCode().compareToIgnoreCase(BUCKET_ALREADY_EXISTS) == 0) {
				return NO_ERROR;
			}

			errCode = ase.getErrorCode();
			switch (errCode) {
			case INVALID_ACCESS_KEY_ID:
				if (isCheck) {
					if (isSource) {
						logger.error(LOG_SOURCE_INVALID_ACCESS);
						errMessage = LOG_SOURCE_INVALID_ACCESS;
					} else {
						logger.error(LOG_TARGET_INVALID_ACCESS);
						errMessage = LOG_TARGET_INVALID_ACCESS;
					}
				} else {
					if (isSource) {
						logger.error(LOG_SOURCE_INVALID_ACCESS);
						errMessage = LOG_SOURCE_INVALID_ACCESS;
					} else {
						logger.error(LOG_TARGET_INVALID_ACCESS);
						errMessage = LOG_TARGET_INVALID_ACCESS;
					}
				}
				return INVALID_ACCESS_KEY;
				
			case SIGNATURE_DOES_NOT_MATCH:
				if (isCheck) {
					if (isSource) {
						logger.error(LOG_SOURCE_INVALID_SECRET);
						errMessage = LOG_SOURCE_INVALID_SECRET;
					} else {
						logger.error(LOG_TARGET_INVALID_SECRET);
						errMessage = LOG_TARGET_INVALID_SECRET;
					}
				} else {
					if (isSource) {
						logger.error(LOG_SOURCE_INVALID_SECRET);
						errMessage = LOG_SOURCE_INVALID_SECRET;
					} else {
						logger.error(LOG_TARGET_INVALID_SECRET);
						errMessage = LOG_TARGET_INVALID_SECRET;
					}
				}
				return INVALID_SECRET_KEY;
			}

			logger.error("{}", ase.getMessage());
			errMessage = ase.getMessage();
			return FAILED_CREATE_BUCKET;
        } catch (IllegalArgumentException e) {
			logger.error(e.getMessage());
			errMessage = e.getMessage();
			return FAILED_CREATE_BUCKET;
		}
	}

	private boolean createBucket(String bucket) {
		try {
			client.createBucket(bucket);
			return true;
		} catch (AmazonServiceException ase) {
			if (ase.getErrorCode().compareToIgnoreCase(BUCKET_ALREADY_OWNED_BY_YOU) == 0) {
				return true;
			} else if (ase.getErrorCode().compareToIgnoreCase(BUCKET_ALREADY_EXISTS) == 0) {
				return true;
			}
			
			logger.error("{} - {}", ase.getErrorCode(), ase.getMessage());
			errMessage = ase.getErrorCode() + " - " + ase.getMessage();
			return false;
        }
	}

    @Override
    public int init(String type) {
		if (config.getEndPoint() == null || config.getEndPoint().isEmpty()) {
			if (isSource) {
				logger.error(LOG_SOURCE_ENDPOINT_NULL);
				errMessage = LOG_SOURCE_ENDPOINT_NULL;
			} else {
				logger.error(LOG_TARGET_ENDPOINT_NULL);
				errMessage = LOG_TARGET_ENDPOINT_NULL;
			}
			// DBManager.insertErrorJob(jobId, errMessage);
			Utils.getDBInstance().insertErrorJob(jobId, errMessage);
			return ENDPOINT_IS_NULL;
		}

		if (!type.equalsIgnoreCase(Repository.SWIFT)) {
			if (config.getBucket() == null || config.getBucket().isEmpty()) {
				if (isSource) {
					logger.error(LOG_SOURCE_BUCKET_NULL);
					errMessage = LOG_SOURCE_BUCKET_NULL;
				} else {
					logger.error(LOG_TARGET_BUCKET_NULL);
					errMessage = LOG_TARGET_BUCKET_NULL;
				}
				// DBManager.insertErrorJob(jobId, errMessage);
				Utils.getDBInstance().insertErrorJob(jobId, errMessage);
				return BUCKET_IS_NULL;
			}
		}

		if (isAWS) {
			isSecure = true;
		} else {
			if (config.getEndPointProtocol().compareToIgnoreCase(HTTPS) == 0) {
				isSecure = true;
			} else {
				isSecure = false;
			}
		}

        int result = getClient();
		if (result != NO_ERROR) {
			// DBManager.insertErrorJob(jobId, errMessage);
			Utils.getDBInstance().insertErrorJob(jobId, errMessage);
			return result;
		}

		result = existBucket(true, config.getBucket());
		if (result == BUCKET_NO_EXIST) {
			if (isSource) {
				logger.error(LOG_SOURCE_BUCKET_NOT_EXIST);
				errMessage = LOG_SOURCE_BUCKET_NOT_EXIST;
				return BUCKET_NO_EXIST;
			} else {
				result = createBucket(false);
				if (result != NO_ERROR) {
					return result;
				}
				logger.info("create bucket {}", config.getBucket());
			}
		} else if (result != NO_ERROR) {
			// DBManager.insertErrorJob(jobId, errMessage);
			logger.error("errMessage : {}", errMessage);
			Utils.getDBInstance().insertErrorJob(jobId, errMessage);
			return result;
		}

        return NO_ERROR;
    }

	@Override
	public boolean isVersioning() {
		try {
			versionConfig = client.getBucketVersioningConfiguration(config.getBucket());
			versioningStatus = versionConfig.getStatus();
			if (versionConfig.getStatus().equals(BucketVersioningConfiguration.OFF)) {
				isVersioning = false;
			} else {
				isVersioning = true;
			}
		} catch (AmazonServiceException ase) {
			logger.error("source bucket versioning - {}", ase.getMessage());
			errMessage = "source bucket versioning - " + ase.getMessage();
			return false;
		}
		return isVersioning;
	}

	public void setVersioning() {
		isVersioning = true;
		client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(config.getBucket(), 
			new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
		logger.info("set target-{} versioning is Enabled", config.getBucket());
	}

	@Override
	public List<String> getBucketList() {
		// not support
		return null;
	}

	@Override
	public boolean createBuckets(List<String> list) {
		for (String bucket : list) {
			if (!createBucket(bucket)) {
				return false;
			}
		}
		return true;
	}

    @Override
    public void makeObjectList(boolean isRerun, boolean targetVersioning) {
		this.targetVersioning = targetVersioning;
        if (isRerun) {
			objectListRerun();
		} else {
			objectList();
		}
    }

	private void objectList() {
		long count = 0;
		ExecutorService executor = Executors.newFixedThreadPool(1000);
		try {
			if (!targetVersioning) {
				ListObjectsRequest request = null;
				if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
					request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix(config.getPrefix());
				} else {
					request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix("");
				}
				ObjectListing result;
				do {
					result = client.listObjects(request);
					for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {		
						count++;
						String tagging = null;
						GetObjectTaggingResult tagResult = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), objectSummary.getKey()));
						if (tagResult != null && !tagResult.getTagSet().isEmpty()) {
							JSONObject json = new JSONObject();
							for (Tag tag: tagResult.getTagSet()) {
								json.put(tag.getKey(), tag.getValue());
							}
							tagging = json.toString();
						}

						DBWorker dbworker = new DBWorker(false,
							isVersioning, 
							jobId, 
							objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/', 
							String.valueOf(objectSummary.getLastModified().getTime()), 
							objectSummary.getSize(), 
							objectSummary.getKey(),
							objectSummary.getETag(),
							tagging);
						executor.execute(dbworker);
					}

					request.setMarker(result.getNextMarker());
				} while (result.isTruncated());
			} else {
				if (isVersioning) {
					ListVersionsRequest request = null;
					if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
						request = new ListVersionsRequest().withBucketName(config.getBucket()).withPrefix(config.getPrefix());
					} else {
						request = new ListVersionsRequest().withBucketName(config.getBucket()).withPrefix("");
					}
					VersionListing listing = null;
					do {
						listing = client.listVersions(request);
						for (S3VersionSummary versionSummary : listing.getVersionSummaries()) {
							count++;
							String tagging = null;
							boolean isFile = versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/';

							if (!versionSummary.isDeleteMarker()) {
								try {
									GetObjectTaggingResult result = null;
									if (isFile) {
										result = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), versionSummary.getKey(), versionSummary.getVersionId()));
									} else {
										result = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), versionSummary.getKey()));
									}
									
									if (result != null && !result.getTagSet().isEmpty()) {
										JSONObject json = new JSONObject();
										for (Tag tag: result.getTagSet()) {
											json.put(tag.getKey(), tag.getValue());
										}
										tagging = json.toString();
									}
								} catch (AmazonServiceException ase) {
									logger.warn("{}:{} : failed get tagging info - {}", versionSummary.getKey(), versionSummary.getVersionId(), ase.getErrorCode());
								}
							}

							DBWorker dbworker = new DBWorker(false,
								isVersioning, 
								jobId, 
								isFile, 
								String.valueOf(versionSummary.getLastModified().getTime()), 
								versionSummary.getSize(), 
								versionSummary.getKey(), 
								versionSummary.getVersionId(), 
								versionSummary.getETag(),
								tagging,
								versionSummary.isDeleteMarker(), 
								versionSummary.isLatest());
							executor.execute(dbworker);
						}
						request.setKeyMarker(listing.getNextKeyMarker());
						request.setVersionIdMarker(listing.getNextVersionIdMarker());
					} while (listing.isTruncated());
				} else {
					ListObjectsRequest request = null;
					if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
						request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix(config.getPrefix());
					} else {
						request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix("");
					}
					ObjectListing result;
					do {
						List<List<Object>> paramList = new ArrayList<List<Object>>();
						result = client.listObjects(request);
						for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {		
							count++;
							String tagging = null;
							GetObjectTaggingResult tagResult = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), objectSummary.getKey()));
							if (tagResult != null && !tagResult.getTagSet().isEmpty()) {
								JSONObject json = new JSONObject();
								for (Tag tag: tagResult.getTagSet()) {
									json.put(tag.getKey(), tag.getValue());
								}
								tagging = json.toString();
							}

							DBWorker dbworker = new DBWorker(false,
								isVersioning, 
								jobId, 
								objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/', 
								String.valueOf(objectSummary.getLastModified().getTime()), 
								objectSummary.getSize(), 
								objectSummary.getKey(),
								objectSummary.getETag(),
								tagging);
							executor.execute(dbworker);
						}

						request.setMarker(result.getNextMarker());
					} while (result.isTruncated());
				}
			}
			executor.shutdown();
			while (!executor.isTerminated()) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (AmazonServiceException ase) {
			Utils.logging(logger, ase);
			logger.error("{} - {}", ase.getErrorCode(), ase.getMessage());
			Utils.getDBInstance().insertErrorJob(jobId, ase.getErrorCode() + "," + ase.getErrorMessage());
			System.exit(-1);
        } catch (AmazonClientException ace) {
			Utils.logging(logger, ace);
        	logger.error("{}", ace.getMessage());
			Utils.getDBInstance().insertErrorJob(jobId, ace.getMessage());
        	System.exit(-1);
        }
	}

	private void objectListRerun() {
		ExecutorService executor = Executors.newFixedThreadPool(1000);
		logger.info("objectListRerun ...");
		try {
			if (!targetVersioning) {
				ListObjectsRequest request = null;
				if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
					request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix(config.getPrefix());
				} else {
					request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix("");
				}

				ObjectListing objectList;
				do {
					objectList = client.listObjects(request);
					for (S3ObjectSummary objectSummary : objectList.getObjectSummaries()) {
						String tagging = null;
						GetObjectTaggingResult result = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), objectSummary.getKey()));
						if (result != null && !result.getTagSet().isEmpty()) {
							JSONObject json = new JSONObject();
							for (Tag tag: result.getTagSet()) {
								json.put(tag.getKey(), tag.getValue());
							}
							tagging = json.toString();
						}

						DBWorker dbworker = new DBWorker(true,
							isVersioning, 
							jobId,
							objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/', 
							String.valueOf(objectSummary.getLastModified().getTime()), 
							objectSummary.getSize(), 
							objectSummary.getKey(), 
							objectSummary.getETag(),
							tagging);

						executor.execute(dbworker);
					}
					request.setMarker(objectList.getNextMarker());
				} while (objectList.isTruncated());
			} else {
				if (isVersioning) {
					ListVersionsRequest request = null;
					if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
						request = new ListVersionsRequest().withBucketName(config.getBucket()).withPrefix(config.getPrefix());
					} else {
						request = new ListVersionsRequest().withBucketName(config.getBucket()).withPrefix("");
					}
	
					VersionListing listing = null;
					do {
						listing = client.listVersions(request);
						for (S3VersionSummary versionSummary : listing.getVersionSummaries()) {
							String tagging = null;
							boolean isFile = versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/';

							if (!versionSummary.isDeleteMarker()) {
								try {
									GetObjectTaggingResult result = null;
									if (isFile) {
										result = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), versionSummary.getKey(), versionSummary.getVersionId()));
									} else {
										result = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), versionSummary.getKey()));
									}
									if (result != null && !result.getTagSet().isEmpty()) {
										JSONObject json = new JSONObject();
										for (Tag tag: result.getTagSet()) {
											json.put(tag.getKey(), tag.getValue());
										}
										tagging = json.toString();
									}
								} catch (AmazonServiceException ase) {
									logger.warn("{}:{} : failed get tagging info - {}", versionSummary.getKey(), versionSummary.getVersionId(), ase.getErrorCode());
								}
							}

							DBWorker dbworker = new DBWorker(true,
								isVersioning, 
								jobId,
								isFile, 
								String.valueOf(versionSummary.getLastModified().getTime()), 
								versionSummary.getSize(), 
								versionSummary.getKey(), 
								versionSummary.getVersionId(), 
								versionSummary.getETag(),
								tagging,
								versionSummary.isDeleteMarker(), 
								versionSummary.isLatest());
								
							executor.execute(dbworker);
						}
						request.setKeyMarker(listing.getNextKeyMarker());
						request.setVersionIdMarker(listing.getNextVersionIdMarker());
					} while (listing.isTruncated());
				} else {
					ListObjectsRequest request = null;
					if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
						request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix(config.getPrefix());
					} else {
						request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix("");
					}
	
					ObjectListing objectList;
					do {
						objectList = client.listObjects(request);
						for (S3ObjectSummary objectSummary : objectList.getObjectSummaries()) {
							String tagging = null;
							GetObjectTaggingResult result = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), objectSummary.getKey()));
							if (result != null && !result.getTagSet().isEmpty()) {
								JSONObject json = new JSONObject();
								for (Tag tag: result.getTagSet()) {
									json.put(tag.getKey(), tag.getValue());
								}
								tagging = json.toString();
							}
	
							DBWorker dbworker = new DBWorker(true,
								isVersioning, 
								jobId,
								objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/', 
								String.valueOf(objectSummary.getLastModified().getTime()), 
								objectSummary.getSize(), 
								objectSummary.getKey(), 
								objectSummary.getETag(),
								tagging);
	
							executor.execute(dbworker);
						}
						request.setMarker(objectList.getNextMarker());
					} while (objectList.isTruncated());
				}
			}
			executor.shutdown();
			while (!executor.isTerminated()) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (AmazonServiceException ase) {
			logger.error("{} - {}", ase.getErrorCode(), ase.getMessage());
			Utils.getDBInstance().insertErrorJob(jobId, ase.getErrorCode() + "," + ase.getErrorMessage());
			System.exit(-1);
        } catch (AmazonClientException ace) {
        	logger.error("{}", ace.getMessage());
			Utils.getDBInstance().insertErrorJob(jobId, ace.getMessage());
        	System.exit(-1);
        }
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
		if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
			path = path.substring(config.getPrefix().length());
			if (path.startsWith("/")) {
				path = path.substring(1);
			}
		}
        return newPath;
    }

	@Override
    public String setTargetPrefix(String path) {
		String newPath = path;
		String prefix = config.getPrefix();
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
				newPath = prefixFinal + path;
			}
		}

        return newPath;
    }

	@Override
	public void setBucketVersioning(String status) {
		if (status == null) {
			return;
		}

		if (status.equals(BucketVersioningConfiguration.SUSPENDED)) {
			client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(config.getBucket(), 
				new BucketVersioningConfiguration(BucketVersioningConfiguration.SUSPENDED)));
			logger.info("set target bucket({}) versioning is Suspended", config.getBucket());
		}
	}

	@Override
    public ObjectData getObject(String path) {
        return null;
    }

	@Override
	public ObjectData getObject(String bucket, String key, String versionId) {
		ObjectData data = new ObjectData();
		GetObjectRequest getObjectRequest = null;
		GetObjectAclRequest getObjectAclRequest = null;
		S3Object s3Object = null;
		
		// if (versionId == null || versionId.equalsIgnoreCase("null")) {
		if (versionId == null) {
			getObjectRequest = new GetObjectRequest(bucket, key);
			getObjectAclRequest = new GetObjectAclRequest(bucket, key);
		} else {
			getObjectRequest = new GetObjectRequest(bucket, key).withVersionId(versionId);
			getObjectAclRequest = new GetObjectAclRequest(bucket, key).withVersionId(versionId);
		}
		s3Object = client.getObject(getObjectRequest);
		data.setS3Object(s3Object);
		data.setMetadata(s3Object.getObjectMetadata());
		data.setInputStream(s3Object.getObjectContent());
		data.setSize(s3Object.getObjectMetadata().getContentLength());
		data.setAcl(client.getObjectAcl(getObjectAclRequest));

		return data;
	}

	@Override
	public ObjectData getObject(String bucket, String key, String versionId, long start) {
		ObjectData data = new ObjectData();
		GetObjectRequest getObjectRequest = null;
		GetObjectAclRequest getObjectAclRequest = null;
		S3Object s3Object = null;

		if (versionId == null) {
			getObjectRequest = new GetObjectRequest(bucket, key).withRange(start);
			getObjectAclRequest = new GetObjectAclRequest(bucket, key);
		} else {
			getObjectRequest = new GetObjectRequest(bucket, key).withVersionId(versionId).withRange(start);
			getObjectAclRequest = new GetObjectAclRequest(bucket, key).withVersionId(versionId);
		}
		s3Object = client.getObject(getObjectRequest);
		data.setS3Object(s3Object);
		data.setMetadata(s3Object.getObjectMetadata());
		data.setInputStream(s3Object.getObjectContent());
		data.setSize(s3Object.getObjectMetadata().getContentLength());
		data.setAcl(client.getObjectAcl(getObjectAclRequest));
		return data;
	}

	@Override
	public ObjectData getObject(String bucket, String key, String versionId, long start, long end) {
		ObjectData data = new ObjectData();
		GetObjectRequest getObjectRequest = null;
		GetObjectAclRequest getObjectAclRequest = null;
		S3Object s3Object = null;

		if (versionId == null) {
			getObjectRequest = new GetObjectRequest(bucket, key).withRange(start, end);
			getObjectAclRequest = new GetObjectAclRequest(bucket, key);
		} else {
			getObjectRequest = new GetObjectRequest(bucket, key).withVersionId(versionId).withRange(start, end);
			getObjectAclRequest = new GetObjectAclRequest(bucket, key).withVersionId(versionId);
		}
		s3Object = client.getObject(getObjectRequest);
		data.setS3Object(s3Object);
		data.setInputStream(s3Object.getObjectContent());
		data.setSize(s3Object.getObjectMetadata().getContentLength());
		data.setAcl(client.getObjectAcl(getObjectAclRequest));
		return data;
	}

	@Override
	public String startMultipart(String bucket, String key, ObjectMetadata objectMetadata) {
		InitiateMultipartUploadResult initMultipart = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key, objectMetadata));
		return initMultipart.getUploadId();
	}

	@Override
	public String uploadPart(String bucket, String key, String uploadId, InputStream is, int partNumber, long partSize) {
		UploadPartResult partResult = client.uploadPart(new UploadPartRequest().withBucketName(bucket).withKey(key)
			.withUploadId(uploadId).withInputStream(is).withPartNumber(partNumber).withPartSize(partSize));
		return partResult.getETag();
	}

	@Override
	public CompleteMultipartUploadResult completeMultipart(String bucket, String key, String uploadId, List<PartETag> list) {
		return client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucket, key, uploadId, list));
	}

	@Override
	public void setTagging(String bucket, String key, String versionId, List<Tag> tagSet) {
		SetObjectTaggingRequest setObjectTaggingRequest = null;
		if (versionId != null) {
			setObjectTaggingRequest = new SetObjectTaggingRequest(bucket, key, versionId, new ObjectTagging(tagSet));
		} else {
			setObjectTaggingRequest = new SetObjectTaggingRequest(bucket, key, new ObjectTagging(tagSet));
		}
		client.setObjectTagging(setObjectTaggingRequest);
	}

	@Override
	public PutObjectResult putObject(boolean isFile, String bucket, String key, ObjectData data, long size) {
		PutObjectRequest putObjectRequest = null;
		if (data.getFile() != null) {
			putObjectRequest = new PutObjectRequest(bucket, key, data.getFile());
		} else {
			if (data.getMetadata() == null) {
				ObjectMetadata meta = new ObjectMetadata();
				meta.setContentLength(data.getSize());
				data.setMetadata(meta);
			}
			putObjectRequest = new PutObjectRequest(bucket, key, data.getInputStream(), data.getMetadata());
		}

		return client.putObject(putObjectRequest);
	}

	@Override
	public void deleteObject(String bucket, String key, String versionId) {
		if (versionId != null) {
			client.deleteVersion(bucket, key, versionId);
		} else {
			client.deleteObject(bucket, key);
		}
	}

	@Override
	public String getVersioningStatus() {
		if (versioningStatus == null) {
			versionConfig = client.getBucketVersioningConfiguration(config.getBucket());
			versioningStatus = versionConfig.getStatus();
		}

		return versioningStatus;
	}

	public ObjectMetadata getMetadata(String bucket, String key, String versionId) {
		GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucket, key).withVersionId(versionId);
		return client.getObjectMetadata(getObjectMetadataRequest);
	}

	class DBWorker implements Runnable {
		private boolean isRerun;
		private boolean isVersioning;
		private String jobId;
		private boolean isFile;
		private String mTime;
		private long size;
		private String path;
		private String versionId;
		private String etag;
		private String tag;
		private boolean isDelete;
		private boolean isLatest;
		// private final Logger logger = LoggerFactory.getLogger(DBWorker.class);

		DBWorker(boolean isRerun, boolean isVersioning, String jobId, boolean isFile, String mTime, long size, String path, String etag, String tag) {
			this.isRerun = isRerun;
			this.isVersioning = isVersioning;
			this.jobId = jobId;
			this.isFile = isFile;
			this.mTime = mTime;
			this.size = size;
			this.path = path;
			this.etag = etag;
			this.tag = tag;
			// logger.info("DB worker, path = {}", path);
		}

		DBWorker(boolean isRerun, boolean isVersioning, String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String tag, boolean isDelete, boolean isLatest) {
			this.isRerun = isRerun;
			this.isVersioning = isVersioning;
			this.jobId = jobId;
			this.isFile = isFile;
			this.mTime = mTime;
			this.size = size;
			this.path = path;
			this.versionId = versionId;
			this.etag = etag;
			this.tag = tag;
			this.isDelete = isDelete;
			this.isLatest = isLatest;
			// logger.info("DB worker, path = {}, versionId = {}", path, versionId);
		}

		@Override
		public void run() {
			if (isRerun) {
				Map<String, String> info = Utils.getDBInstance().infoExistObjectVersion(jobId, path, versionId);
				if (info.isEmpty()) {
					if (isVersioning) {
						Utils.insertRerunMoveObjectVersion(jobId, 
							isFile,
							mTime,
							size,
							path,
							versionId,
							etag,
							null,
							tag,
							isDelete,
							isLatest);
					} else {
						Utils.insertRerunMoveObject(jobId, 
							isFile,
							mTime,
							size,
							path,
							etag,
							null,
							tag);
					}
					Utils.updateJobRerunInfo(jobId, size);
				} else {
					int state = Integer.parseInt(info.get(MariaDB.MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE));
					String savedMTime = info.get(MariaDB.MOVE_OBJECTS_TABLE_COLUMN_MTIME);
					String savedEtag = info.get(MariaDB.MOVE_OBJECTS_TABLE_COLUMN_ETAG);
					
					if (state == 3 && this.etag == null && savedEtag.compareTo("0") == 0) {
						if (isVersioning) {
							Utils.updateRerunSkipObjectVersion(jobId, path, versionId, isLatest);
						} else {
							Utils.updateRerunSkipObject(jobId, path);
						}
						Utils.updateJobRerunSkipInfo(jobId, size);
					} else if (state == 3 && savedMTime.compareTo(this.mTime) == 0 && savedEtag.compareTo(this.etag) == 0) {
						if (isVersioning) {
							Utils.updateRerunSkipObjectVersion(jobId, path, versionId, isLatest);
						} else {
							Utils.updateRerunSkipObject(jobId, path);
						}
						Utils.updateJobRerunSkipInfo(jobId, size);
					} else {
						if (isVersioning) {
							Utils.updateToMoveObjectVersion(jobId, this.mTime, size, path, versionId);
						} else {
							Utils.updateToMoveObject(jobId, this.mTime, size, path);
						}
						Utils.updateJobRerunInfo(jobId, size);
					}
				}
			} else {
				if (isVersioning) {
					Utils.insertMoveObjectVersion(jobId, 
						isFile, 
						mTime, 
						size, 
						path, 
						versionId,  
						etag, 
						null,
						tag,
						isDelete, 
						isLatest);
				} else {
					Utils.insertMoveObject(jobId, 
						isFile, 
						mTime, 
						size, 
						path,
						etag,
						tag);
				}
				Utils.updateJobInfo(jobId, size);
			}
		}
	}

	class DBWorkerTaget implements Runnable {
		private String jobId;
		private String path;
		private String versionId;
		private long size;
		private String etag;

		DBWorkerTaget(String jobId, String path, String versionId, long size, String etag) {
			this.jobId = jobId;
			this.path = path;
			this.versionId = versionId;
			this.size = size;
			this.etag = etag;
		}

		@Override
		public void run() {
			Utils.insertTargetObject(jobId, path, versionId, size, etag);
		}
	}

	@Override
	public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) {
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, input, metadata);
		return client.putObject(putObjectRequest);
	}

	@Override
	public void makeTargetObjectList(boolean targetVersioning) {
		if (config.isTargetSync()) {
			ExecutorService executor = Executors.newFixedThreadPool(1000);
			
			try {
				if (!targetVersioning) {
					ListObjectsRequest request = null;
					if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
						request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix(config.getPrefix());
					} else {
						request = new ListObjectsRequest().withBucketName(config.getBucket()).withPrefix("");
					}
					ObjectListing result;
					do {
						result = client.listObjects(request);
						for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {		
							DBWorkerTaget dbworker = new DBWorkerTaget(jobId, objectSummary.getKey(), "null", objectSummary.getSize(), objectSummary.getETag());
							executor.execute(dbworker);
						}
						request.setMarker(result.getNextMarker());
					} while (result.isTruncated());
				} else {
					ListVersionsRequest request = null;
					if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
						request = new ListVersionsRequest().withBucketName(config.getBucket()).withPrefix(config.getPrefix());
					} else {
						request = new ListVersionsRequest().withBucketName(config.getBucket()).withPrefix("");
					}
					VersionListing listing = null;
					do {
						listing = client.listVersions(request);
						for (S3VersionSummary versionSummary : listing.getVersionSummaries()) {
							if (!versionSummary.isDeleteMarker()) {
								DBWorkerTaget dbworker = new DBWorkerTaget(jobId, versionSummary.getKey(), versionSummary.getVersionId(), versionSummary.getSize(), versionSummary.getETag());
								executor.execute(dbworker);
							}
						}
						request.setKeyMarker(listing.getNextKeyMarker());
						request.setVersionIdMarker(listing.getNextVersionIdMarker());
					} while (listing.isTruncated());
				}
				executor.shutdown();
				while (!executor.isTerminated()) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} catch (AmazonServiceException ase) {
				Utils.logging(logger, ase);
				logger.error("make target object list failed. {} - {}", ase.getErrorCode(), ase.getMessage());
			} catch (AmazonClientException ace) {
				Utils.logging(logger, ace);
				logger.error("make target object list failed. {}", ace.getMessage());
			}
		}
	}

	@Override
	public boolean isTargetSync() {
		return isTargetSync;
	}

	@Override
	public SyncMode getTargetSyncMode() {
		return targetSyncMode;
	}

	@Override
	public void setAcl(String bucket, String key, String versionId, AccessControlList acl) {
		// TODO Auto-generated method stub
		SetObjectAclRequest setObjectAclRequest = null;
		if (acl.getGrantsAsList().size() == 0) {
			if (versionId != null) {
				setObjectAclRequest = new SetObjectAclRequest(bucket, key, versionId, CannedAccessControlList.Private);
			} else {
				setObjectAclRequest = new SetObjectAclRequest(bucket, key, CannedAccessControlList.Private);
			}
		} else {
			if (versionId != null) {
				setObjectAclRequest = new SetObjectAclRequest(bucket, key, versionId, acl);
			} else {
				setObjectAclRequest = new SetObjectAclRequest(bucket, key, acl);
			}
		}
		client.setObjectAcl(setObjectAclRequest);
	}

	@Override
	public AccessControlList getAcl(String bucket, String key, String versionId) {
		GetObjectAclRequest getObjectAclRequest = null;
		if (versionId != null) {
			getObjectAclRequest = new GetObjectAclRequest(bucket, key).withVersionId(versionId);
		} else {
			getObjectAclRequest = new GetObjectAclRequest(bucket, key);
		}
		return client.getObjectAcl(getObjectAclRequest);
	}
}
