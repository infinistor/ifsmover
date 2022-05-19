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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

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
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ifs_mover.Config;
import ifs_mover.DBManager;
import ifs_mover.Utils;

public class IfsS3 implements Repository, S3 {
	private static final Logger logger = LoggerFactory.getLogger(IfsS3.class);
	private String jobId;
	private boolean isSource;
    private Config  config;
    private boolean isAWS;
    private boolean isSecure;
	private boolean isVersioning;
	private String versioningStatus;
    private AmazonS3 client;
	private String errCode;
	private String errMessage;
	private BucketVersioningConfiguration versionConfig;

	private final String HTTPS = "https";
	private final String AWS_S3_V4_SIGNER_TYPE = "AWSS3V4SignerType";
	private final String BUCKET_ALREADY_OWNED_BY_YOU = "BucketAlreadyOwnedByYou";
	private final String BUCKET_ALREADY_EXISTS = "BucketAlreadyExists";
	private final String INVALID_ACCESS_KEY_ID = "InvalidAccessKeyId";
	private final String SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch";

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

	IfsS3(String jobId) {
		this.jobId = jobId;
	}

    @Override
    public void setConfig(Config config, boolean isSource) {
        this.config = config;
		this.isSource = isSource;
        isAWS = config.isAWS();
        isSecure = isAWS;
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
				result = NO_ERROR;
			} else {
				result = BUCKET_NO_EXIST;
			}
		} catch (AmazonServiceException ase) {
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
				result = INVALID_ACCESS_KEY;
				break;
				
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
				result = INVALID_SECRET_KEY;
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
			DBManager.insertErrorJob(jobId, errMessage);
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
				DBManager.insertErrorJob(jobId, errMessage);
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
			DBManager.insertErrorJob(jobId, errMessage);
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
			DBManager.insertErrorJob(jobId, errMessage);
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
    public void makeObjectList(boolean isRerun) {
        if (isRerun) {
			objectListRerun();
		} else {
			objectList();
		}
    }

	private void objectList() {
		long count = 0;
		try {
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
						if (!versionSummary.isDeleteMarker()) {
							try {
								GetObjectTaggingResult result = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), versionSummary.getKey(), versionSummary.getVersionId()));
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
						
						Utils.insertMoveObjectVersion(jobId, versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/', 
							versionSummary.getLastModified().toString(), 
							versionSummary.getSize(), 
							versionSummary.getKey(), 
							versionSummary.getVersionId(), 
							versionSummary.getETag(),
							"",
							tagging,
							versionSummary.isDeleteMarker(), 
							versionSummary.isLatest());
						Utils.updateJobInfo(jobId, versionSummary.getSize());
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
						
						Utils.insertMoveObject(jobId, objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/', 
							objectSummary.getLastModified().toString(), 
							objectSummary.getSize(), 
							objectSummary.getKey(),
							objectSummary.getETag(),
							tagging);
						Utils.updateJobInfo(jobId, objectSummary.getSize());
					}
					request.setMarker(result.getNextMarker());
				} while (result.isTruncated());
			}
		} catch (AmazonServiceException ase) {
			Utils.logging(logger, ase);
			logger.error("{} - {}", ase.getErrorCode(), ase.getMessage());
			DBManager.insertErrorJob(jobId, ase.getErrorCode() + "," + ase.getErrorMessage());
			System.exit(-1);
        } catch (AmazonClientException ace) {
			Utils.logging(logger, ace);
        	logger.error("{}", ace.getMessage());
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

	private void objectListRerun() {
		int state = 0;
		try {
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
						if (!versionSummary.isDeleteMarker()) {
							try {
								GetObjectTaggingResult result = client.getObjectTagging(new GetObjectTaggingRequest(config.getBucket(), versionSummary.getKey(), versionSummary.getVersionId()));
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
						Map<String, String> info = DBManager.infoExistObjectVersion(jobId, versionSummary.getKey(), versionSummary.getVersionId());
						if (info.isEmpty()) {
							Utils.insertRerunMoveObjectVersion(jobId, versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/', 
								versionSummary.getLastModified().toString(), 
								versionSummary.getSize(), 
								versionSummary.getKey(), 
								versionSummary.getVersionId(), 
								versionSummary.getETag(),
								"",
								tagging,
								versionSummary.isDeleteMarker(), 
								versionSummary.isLatest());
							Utils.updateJobRerunInfo(jobId, versionSummary.getSize());
						} else {
							state = Integer.parseInt(info.get(DBManager.MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE));
							String mTime = info.get(DBManager.MOVE_OBJECTS_TABLE_COLUMN_MTIME);
							if (state == 3 && mTime.compareTo(versionSummary.getLastModified().toString()) == 0) {	
								Utils.updateRerunSkipObjectVersion(jobId, versionSummary.getKey(), versionSummary.getVersionId());
								Utils.updateJobRerunSkipInfo(jobId, versionSummary.getSize());
							} else {
								Utils.updateToMoveObjectVersion(jobId, versionSummary.getLastModified().toString(), versionSummary.getSize(), versionSummary.getKey(), versionSummary.getVersionId());
								Utils.updateJobRerunInfo(jobId, versionSummary.getSize());
							}
						}
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

						Map<String, String> info = DBManager.infoExistObject(jobId, objectSummary.getKey());
						if (info.isEmpty()) {
							Utils.insertRerunMoveObject(jobId, objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/', 
								objectSummary.getLastModified().toString(), 
								objectSummary.getSize(), 
								objectSummary.getKey(),
								objectSummary.getETag(),
								"",
								tagging);
								Utils.updateJobRerunInfo(jobId, objectSummary.getSize());
						} else {
							state = Integer.parseInt(info.get(DBManager.MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE));
							String mTime = info.get(DBManager.MOVE_OBJECTS_TABLE_COLUMN_MTIME);
							if (state == 3 && mTime.compareTo(objectSummary.getLastModified().toString()) == 0) {
								Utils.updateRerunSkipObject(jobId, objectSummary.getKey());
								Utils.updateJobRerunSkipInfo(jobId, objectSummary.getSize());
							} else {
								Utils.updateToMoveObject(jobId, objectSummary.getLastModified().toString(), objectSummary.getSize(), objectSummary.getKey());
								Utils.updateJobRerunInfo(jobId, objectSummary.getSize());
							}
						}
					}
					request.setMarker(objectList.getNextMarker());
				} while (objectList.isTruncated());
			}
		} catch (AmazonServiceException ase) {
			logger.error("{} - {}", ase.getErrorCode(), ase.getMessage());
			DBManager.insertErrorJob(jobId, ase.getErrorCode() + "," + ase.getErrorMessage());
			System.exit(-1);
        } catch (AmazonClientException ace) {
        	logger.error("{}", ace.getMessage());
        	DBManager.insertErrorJob(jobId, ace.getMessage());
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
		S3Object s3Data = null;
		
		if (versionId == null || versionId.equalsIgnoreCase("null")) {
			getObjectRequest = new GetObjectRequest(bucket, key);
		} else {
			getObjectRequest = new GetObjectRequest(bucket, key).withVersionId(versionId);
		}
		s3Data = client.getObject(getObjectRequest);
		data.setMetadata(s3Data.getObjectMetadata());
		data.setInputStream(s3Data.getObjectContent());
		data.setSize(s3Data.getObjectMetadata().getContentLength());

		return data;
	}

	@Override
	public ObjectData getObject(String bucket, String key, String versionId, long start, long end) {
		ObjectData data = new ObjectData();
		GetObjectRequest getObjectRequest = null;
		S3Object s3Data = null;

		if (versionId == null) {
			getObjectRequest = new GetObjectRequest(bucket, key).withRange(start, end);
		} else {
			getObjectRequest = new GetObjectRequest(bucket, key).withVersionId(versionId).withRange(start, end);
		}
		s3Data = client.getObject(getObjectRequest);
		data.setInputStream(s3Data.getObjectContent());
		data.setSize(s3Data.getObjectMetadata().getContentLength());
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
	public String completeMultipart(String bucket, String key, String uploadId, List<PartETag> list) {
		return client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucket, key, uploadId, list)).getETag();
	}

	@Override
	public void setTagging(String bucket, String key, List<Tag> tagSet) {
		client.setObjectTagging(new SetObjectTaggingRequest(bucket, key, new ObjectTagging(tagSet)));
	}

	@Override
	public String putObject(boolean isFile, String bucket, String key, ObjectData data, long size) {
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
		return client.putObject(putObjectRequest).getETag();
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
}
