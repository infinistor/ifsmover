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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import ifs_mover.db.MoverDB;
import ifs_mover.repository.IfsS3;
import ifs_mover.repository.ObjectData;
import ifs_mover.repository.Repository;
import ifs_mover.repository.RepositoryFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.Tag;

public class ObjectMover {
	private static final Logger logger = LoggerFactory.getLogger(ObjectMover.class);
	
	private Config sourceConfig;
	private Config targetConfig;
	private MoverConfig moverConfig;
	private int threadCount;
	private String type;
	private boolean isRerun;
	private boolean isVersioning;
	private String jobId;
	private Repository sourceRepository;
	private IfsS3 targetRepository;
	private boolean targetVersioning;
	private long partSize;
	private long useMultipartSize;
	private String replaceChars;
	private boolean isSetTagetPathToLowerCase;
	// private String inventoryFileName;

	private final int GET_OBJECTS_LIMIT = 1000;

	private final String NO_SUCH_KEY = "NoSuchKey";
	private final String NOT_FOUND = "Not Found";
	
	public static class Builder {
		private Config sourceConfig;
		private Config targetConfig;
		private MoverConfig moverConfig;
		private int threadCount;
		private String type;
		private boolean isRerun;
		private String jobId;
		// private String inventoryFileName;
		
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

		public Builder moverConfig(MoverConfig moverConfig) {
			this.moverConfig = moverConfig;
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
		
		public Builder isRerun(boolean isRerun) {
			this.isRerun = isRerun;
			return this;
		}

		// public Builder inventoryFileName(String inventoryFileName) {
		// 	this.inventoryFileName = inventoryFileName;
		// 	return this;
		// }
		
		public ObjectMover build() {
			return new ObjectMover(this);
		}
	}
	
	private ObjectMover(Builder builder) {
		sourceConfig = builder.sourceConfig;
		targetConfig = builder.targetConfig;
		moverConfig = builder.moverConfig;
		threadCount = builder.threadCount;
		type = builder.type;
		isRerun = builder.isRerun;
		jobId = builder.jobId;
		// inventoryFileName = builder.inventoryFileName;

		RepositoryFactory factory = new RepositoryFactory();
		sourceRepository = factory.getSourceRepository(type, jobId);
		sourceRepository.setConfig(sourceConfig, true);
		targetRepository = factory.getTargetRepository(jobId);
		targetRepository.setConfig(targetConfig, false);

		partSize = sourceConfig.getPartSize();
		useMultipartSize = sourceConfig.getUseMultipartSize();
		replaceChars = moverConfig.getReplaceChars();
		isSetTagetPathToLowerCase = moverConfig.isSetTagetPathToLowerCase();
		logger.info("multipart size : {}, part size : {}, thread count: {}", useMultipartSize, partSize, threadCount);
	}
	
	public void check() {
		int result = sourceRepository.check(type);
		if (result != Repository.NO_ERROR) {
			System.out.println("Error : " + sourceRepository.getErrMessage());
			System.exit(-1);
		}
		result = targetRepository.check(type);
		if (result != Repository.NO_ERROR) {
			System.out.println("Error : " + targetRepository.getErrMessage());
			System.exit(-1);
		}
	}
	
	public void init() {
		int result = sourceRepository.init(type);
		if (result != Repository.NO_ERROR) {
			Utils.getDBInstance().insertErrorJob(jobId, sourceRepository.getErrMessage());
			System.out.println(sourceRepository.getErrMessage());
			System.exit(-1);
		}

		result = targetRepository.init(type);
		if (result != Repository.NO_ERROR) {
			Utils.getDBInstance().insertErrorJob(jobId, targetRepository.getErrMessage());
			System.out.println("Error : " + targetRepository.getErrMessage());
			System.exit(-1);
		}

		// make target bucket for swift
		if (type.equalsIgnoreCase(Repository.SWIFT)) {
			List<String> bucketList = sourceRepository.getBucketList();
			if (bucketList != null && !targetRepository.createBuckets(bucketList)) {
				System.out.println("Error : " + targetRepository.getErrMessage());
				System.exit(-1);
			}
		}

		// check target conf versioning
		// if (targetConfig.getVersoning() == null || targetConfig.getVersoning().isEmpty()) {
		// 	targetVersioning = true;
		// } else if (targetConfig.getVersoning().compareToIgnoreCase("OFF") == 0) {
		// 	targetVersioning = false;
		// } else {
		// 	targetVersioning = true;
		// }

		// check source bucket versioning
		// if (sourceRepository.isVersioning()) {
			// check target bucket versioning
		// 	if (!targetRepository.isVersioning()) {
		// 		if (targetVersioning) {
		// 			isVersioning = true;
		// 			targetRepository.setVersioning();
		// 		} else {
		// 			isVersioning = false;
		// 		}
		// 	} else {
		// 		isVersioning = true;
		// 		targetVersioning = true;
		// 	}
		// } else {
		// 	isVersioning = false;
		// 	targetVersioning = false;
		// }

		isVersioning = false;
		targetVersioning = false;

		logger.info("isVersioning : {}, targetVersioning : {}", isVersioning, targetVersioning);
		if (targetConfig.isTargetSync()) {
			if (targetConfig.getSyncMode() == SyncMode.ETAG) {
				logger.info("target sync mode : check etag.");
			} else if (targetConfig.getSyncMode() == SyncMode.SIZE) {
				logger.info("target sync mode : check size.");
			} else if (targetConfig.getSyncMode() == SyncMode.EXIST) {
				logger.info("target sync mode : check exist.");
			}
		}

		// logger.info("check bucket encryption.");
		// checkBucketEncrytion();
		// sourceRepository.getBucketPolicy();

		// if (inventoryFileName != null && !inventoryFileName.isEmpty()) {
		// 	sourceRepository.makeObjectList(isRerun, targetVersioning, inventoryFileName);
		// } else {
		// 	sourceRepository.makeObjectList(isRerun, targetVersioning);
		// }

		sourceRepository.makeObjectList(isRerun, targetVersioning);

		if (isRerun) {
			long start = System.currentTimeMillis();
			Utils.getDBInstance().updateSkipRerun(jobId);
			Utils.getDBInstance().infoSkipRerun(jobId);
			Utils.getDBInstance().checkDeleteObjectsForReRun(jobId);
			long end = System.currentTimeMillis();
			logger.info("skip check time : {} ms", end - start);
		}
		
		targetRepository.makeTargetObjectList(targetVersioning);
	}

	// public void checkBucketEncrytion() {
	// 	ServerSideEncryptionConfiguration sse = sourceRepository.getBucketEncryption();
	// 	if (sse != null) {
	// 		targetRepository.setBucketEncryption(sse);
	// 	}
	// }

	public void moveObjects() {		
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);

		try {
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
				logger.info("target prefix : {}", prefixFinal);
				if (prefixFinal.length() >= 1) {
					try {
						ObjectMetadata meta = new ObjectMetadata();
						meta.setContentLength(0);
						InputStream is = new ByteArrayInputStream(new byte[0]);
						ObjectData data = new ObjectData(meta, is);
						targetRepository.putObject(targetRepository.getClient(), false, targetConfig.getBucket(), prefixFinal, data, 0L);
						data.getInputStream().close();
						logger.info("make target prefix {}", prefixFinal);
					} catch (AmazonServiceException ase) {
						logger.warn("{} {} {}", prefixFinal, ase.getErrorCode(), ase.getErrorMessage());
					} catch (AmazonClientException ace) {
						logger.warn("{} {}", prefixFinal, ace.getMessage());
					} catch (IOException e) {
						logger.warn("{} {}", prefixFinal, e.getMessage());
					}
				}
			}

			for (int i = 0; i < threadCount; i++) {
				Mover mover = new Mover(i, isRerun);
				executor.execute(mover);
			}
			executor.shutdown();

			while (true) {
				if (executor.isTerminated()) {
					logger.info("End of moving jobs");
					return;
				} else {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}
				}
			}
		} catch (Exception e) {
			Utils.logging(logger, e);
		}
	}
	class Mover implements Runnable {
		final Logger logger = LoggerFactory.getLogger(Mover.class);
		private int threadNumber;
		private boolean isRerun;
		private AmazonS3 sourceS3Client;
		private AmazonS3 targetS3Client;

		List<MoveData> latestList = new ArrayList<MoveData>();
		List<MoveData> deletedList = new ArrayList<MoveData>();
		
		public Mover(int threadNumber, boolean isRerun) {
			this.threadNumber = threadNumber;
			this.isRerun = isRerun;
			if (type.equalsIgnoreCase(Repository.S3)) {
				sourceS3Client = sourceRepository.createS3Clients();
			}
			targetS3Client = targetRepository.createS3Clients();
		}
		
		private MoveResult moveObject(String path, boolean isDelete, boolean isLatest, boolean isFile, String versionId, String etag, String multipartInfo, long size) {
			String sourcePath = null;
			String targetPath = null;
			String sourceBucket = null;
			String targetBucket = null;

			if (type.equalsIgnoreCase(Repository.SWIFT)) {
				sourceBucket = path.split("/", 2)[0];
				sourcePath = path.split("/", 2)[1];
				targetPath = path.split("/", 2)[1];
				targetPath = targetRepository.setTargetPrefix(targetPath);
				if (!isFile && !targetPath.endsWith("/")) {
					targetPath += "/";
				}
				if (!Utils.isValidBucketName(sourceBucket)) {
					targetBucket = Utils.getS3BucketName(sourceBucket);
				} else {
					targetBucket = sourceBucket;
				}
			} else {
				sourceBucket = sourceConfig.getBucket();
				sourcePath = sourceRepository.setPrefix(path);
				targetBucket = targetConfig.getBucket();
				if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
					targetPath = targetRepository.setTargetPrefix(sourceRepository.setTargetPrefix(path));
				} else {
					targetPath = targetRepository.setTargetPrefix(path);
				}
				if (!isFile && !targetPath.endsWith("/")) {
					targetPath += "/";
				}
			}

			if (replaceChars != null && !replaceChars.isEmpty()) {
				targetPath = targetPath.replaceAll(replaceChars, "-");
			}

			if (isSetTagetPathToLowerCase) {
				targetPath = targetPath.toLowerCase();
			}

			if (versionId != null && versionId.compareTo("0") == 0) {
				versionId = null;
			}

			try {
				if (isDelete) {
					if (isLatest) {
						targetRepository.deleteObject(targetS3Client, targetBucket, targetPath, null);
						if (isRerun) {
							Utils.getDBInstance().updateRerunDeleteMarker(jobId, path, versionId);
						} else {
							Utils.getDBInstance().updateDeleteMarker(jobId, path, versionId);
						}
						return MoveResult.DELETE_SUCCESS;
					}
				} else {
					// Check if it is the same as target object
					if (targetRepository.isTargetSync()) {
						if (targetRepository.getTargetSyncMode() == SyncMode.ETAG && type.equalsIgnoreCase(Repository.S3)) { // check ETAG
							if (Utils.getDBInstance().compareObject(jobId, path, etag)) {
								logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
								Utils.updateSkipObject(jobId, path, versionId);
								return MoveResult.SKIP;
							}
						} else if (targetRepository.getTargetSyncMode() == SyncMode.SIZE) {	// check SIZE
							if (type.equalsIgnoreCase(Repository.S3)) {
								if (Utils.getDBInstance().compareObject(jobId, path, size)) {
									logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
									Utils.updateSkipObject(jobId, path, versionId);
									return MoveResult.SKIP;
								}
							} else if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
								logger.debug("path : {}", targetPath);
								if (Utils.getDBInstance().compareObject(jobId, targetPath, size)) {
									logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
									Utils.updateSkipObject(jobId, path, versionId);
									return MoveResult.SKIP;
								}
							}
						} else if (targetRepository.getTargetSyncMode() == SyncMode.EXIST) { // check EXIST
							if (type.equalsIgnoreCase(Repository.S3)) {
								if (Utils.getDBInstance().isExistObject(jobId, path)) {
									logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
									Utils.updateSkipObject(jobId, path, versionId);
									return MoveResult.SKIP;
								}
							} else if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
								logger.debug("path : {}", targetPath);
								if (Utils.getDBInstance().isExistObject(jobId, targetPath)) {
									logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
									Utils.updateSkipObject(jobId, path, versionId);
									return MoveResult.SKIP;
								}
							}
						}
					}

					if (isFile) {
						if (multipartInfo != null && !multipartInfo.isEmpty()) {
							// for swift large file (more than 5G)
							String uploadId = targetRepository.startMultipart(targetS3Client, targetBucket, targetPath, null);
							List<PartETag> partList = new ArrayList<PartETag>();
							String[] multiPath = multipartInfo.split("/", 2);
							int partNumber = 0;
							ObjectData data = null;
							do {
								String partPath = String.format("%08d", partNumber++);
								partPath = multiPath[1] + partPath;
								if (data != null) {
									data.getS3Object().close();
								}
								data = sourceRepository.getObject(sourceS3Client, multiPath[0], partPath, null);
								if (data != null) {
									String partETag = targetRepository.uploadPart(targetS3Client, targetBucket, targetPath, uploadId, data.getInputStream(), partNumber, data.getSize());
									partList.add(new PartETag(partNumber, partETag));
									data.getInputStream().close();
								}
							} while (data != null);

							CompleteMultipartUploadResult completeMultipartUploadResult = targetRepository.completeMultipart(targetS3Client, targetBucket, targetPath, uploadId, partList);
							// if (tagSet != null && tagSet.size() > 0) {
							// 	targetRepository.setTagging(targetBucket, targetPath, completeMultipartUploadResult.getVersionId(), tagSet);
							// }
							logger.info("move success : {}", path);
						} else if ((!type.equalsIgnoreCase(Repository.IFS_FILE) && size >= useMultipartSize)) {
							// send multipart
							if (versionId != null && !versionId.isEmpty()) {
								logger.debug("send multipart : {}:{}, size {}", path, versionId, size);
							} else {
								logger.debug("send multipart : {}, size {}", path, size);
							}
							
							long limitSize = partSize;

							ObjectMetadata objectMetadata = sourceRepository.getMetadata(sourceS3Client, sourceBucket, sourcePath, versionId);
							AccessControlList objectAcl = sourceRepository.getAcl(sourceS3Client, sourceBucket, sourcePath, versionId);
							List<Tag> tagSet = sourceRepository.getTagging(sourceS3Client, sourceBucket, sourcePath, versionId);
							String uploadId = targetRepository.startMultipart(targetS3Client, targetBucket, targetPath, objectMetadata);
							List<PartETag> partList = new ArrayList<PartETag>();
							int partNumber = 1;

							for (long i = 0; i < size; i += limitSize, partNumber++) {
								long start = i;
								long end = i + limitSize - 1;
								if (end >= size) {
									end = size - 1;
								}

								ObjectData data = sourceRepository.getObject(sourceS3Client, sourceBucket, sourcePath, versionId, start, end);

								if (data != null) {
									String partETag = targetRepository.uploadPart(targetS3Client, targetBucket, targetPath, uploadId, data.getInputStream(), partNumber, data.getSize());
									partList.add(new PartETag(partNumber, partETag));
									data.close();
									logger.info("{} - move part : {}, size : {}", path, partNumber, data.getSize());
								}
							}

							CompleteMultipartUploadResult completeMultipartUploadResult = targetRepository.completeMultipart(targetS3Client, targetBucket, targetPath, uploadId, partList);
							targetRepository.setAcl(targetS3Client, targetBucket, targetPath, completeMultipartUploadResult.getVersionId(), objectAcl);

							if (tagSet != null && !tagSet.isEmpty()) {
								targetRepository.setTagging(targetS3Client, targetBucket, targetPath, completeMultipartUploadResult.getVersionId(), tagSet);
							}
						} else {
							ObjectData data = null;
							String s3ETag = null;
							PutObjectResult putObjectResult = null;
							List<Tag> tagSet = null;
							if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
								data = sourceRepository.getObject(sourcePath);
								putObjectResult = targetRepository.putObject(targetS3Client, isFile, targetBucket, targetPath, data, data.getSize());
								s3ETag = putObjectResult.getETag();
								data.close();
							} else {
								tagSet = sourceRepository.getTagging(sourceS3Client, sourceBucket, sourcePath, versionId);
								if (size > 0) {
									data = sourceRepository.getObject(sourceS3Client, sourceBucket, sourcePath, versionId, 0);
									if (data == null) {
										logger.warn("not found : {} {}", sourceBucket, sourcePath);
										return MoveResult.MOVE_FAILURE;
									}
									putObjectResult = targetRepository.putObject(targetS3Client, isFile, targetBucket, targetPath, data, data.getSize());
									s3ETag = putObjectResult.getETag();
									data.close();
								} else {
									ObjectMetadata meta = sourceRepository.getMetadata(sourceS3Client, sourceBucket, sourcePath, versionId);
									AccessControlList acl = sourceRepository.getAcl(sourceS3Client, sourceBucket, sourcePath, versionId);
									InputStream is = new ByteArrayInputStream(new byte[0]);
									logger.debug("size : {}, meta size : {}", size, meta.getContentLength());
									putObjectResult = targetRepository.putObject(targetS3Client, targetBucket, targetPath, is, meta);
									is.close();
									targetRepository.setAcl(targetS3Client, targetBucket, targetPath, putObjectResult.getVersionId(), acl);
								}
							}

							if (tagSet != null && !tagSet.isEmpty()) {
								targetRepository.setTagging(targetS3Client, targetBucket, targetPath, putObjectResult.getVersionId(), tagSet);
							}

							if (data != null && data.getAcl() != null) {
								targetRepository.setAcl(targetS3Client, targetBucket, targetPath, putObjectResult.getVersionId(), data.getAcl());
							}

							if (!type.equalsIgnoreCase(Repository.IFS_FILE) && isFile && size > 0) {
								if (!etag.equals(s3ETag) && !etag.contains("-")) {
									logger.warn("{}:{} -- The etags are different. source : {}, target : {}", path, versionId, etag, s3ETag);
								} 
							} 
						}
					} else {	// move dir
						ObjectData data;
						if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
							ObjectMetadata meta = new ObjectMetadata();
							meta.setContentLength(0);
							InputStream is = new ByteArrayInputStream(new byte[0]);
							data = new ObjectData(meta, is);
							targetRepository.putObject(targetS3Client, isFile, targetBucket, targetPath, data, 0L);
							is.close();
						} else {
							if (versionId == null || (versionId != null && versionId.compareToIgnoreCase("null") == 0)) {
								ObjectMetadata meta = sourceRepository.getMetadata(sourceS3Client, sourceBucket, sourcePath, versionId);
								AccessControlList acl = sourceRepository.getAcl(sourceS3Client, sourceBucket, sourcePath, versionId);
								List<Tag> tagSet = sourceRepository.getTagging(sourceS3Client, sourceBucket, sourcePath, versionId);
								InputStream is = new ByteArrayInputStream(new byte[0]);
								PutObjectResult putObjectResult = targetRepository.putObject(targetS3Client, targetBucket, targetPath, is, meta);
								is.close();
								if (tagSet != null && !tagSet.isEmpty()) {
									targetRepository.setTagging(targetS3Client, targetBucket, targetPath, putObjectResult.getVersionId(), tagSet);
								}
								targetRepository.setAcl(targetS3Client, targetBucket, targetPath, putObjectResult.getVersionId(), acl);
							} else {
								logger.info("Ignore this object because Because it is a directory with versionid, path : {}, versionId : {}", path, versionId);
								Utils.updateSkipObject(jobId, path, versionId);
								return MoveResult.SKIP;
							}
						}
					}
				}
			} catch (AmazonServiceException ase) {
				Utils.logging(logger, ase);
				if (ase.getErrorCode().compareToIgnoreCase(NO_SUCH_KEY) == 0) {
					logger.warn("{} {}", path, ase.getErrorMessage());
				} else if (ase.getErrorMessage().contains(NOT_FOUND)) {
					logger.warn("{} {}", path, ase.getErrorMessage());
				} else {
					logger.warn("{} {} - {}", path, ase.getErrorCode(), ase.getErrorMessage());
				}
				return MoveResult.MOVE_FAILURE;
			} catch (AmazonClientException ace) {
				Utils.logging(logger, ace);
				logger.warn("{} {}", path, ace.getMessage());
				return MoveResult.MOVE_FAILURE;
			} catch (Exception e) {
				Utils.logging(logger, e);
				return MoveResult.MOVE_FAILURE;
			}

			return MoveResult.MOVE_SUCCESS;
		}
	
		private MoveResult retryMoveObject(String path, boolean isDelete, boolean isLatest, boolean isFile, String versionId, String etag, String multipartInfo, long size) {
			for (int i = 0; i < Utils.RETRY_COUNT; i++) {
				MoveResult moveResult = moveObject(path, isDelete, isLatest, isFile, versionId, etag, multipartInfo, size);
				if (moveResult != MoveResult.MOVE_FAILURE) {
					return moveResult;
				}
			}
			return MoveResult.MOVE_FAILURE;
		}

		@Override
		public void run() {
			try {
				MDC.clear();
				MDC.put("logFileName", "ifs_mover." + jobId + ".log");

				long maxSequence = Utils.getDBInstance().getMaxSequence(jobId);
				long sequence = threadNumber * GET_OBJECTS_LIMIT;

				while (true) {
					long start = System.currentTimeMillis();
					List<MoveData> moveList = null;
					if (!isRerun) {
						moveList = Utils.getDBInstance().getToMoveObjectsInfo(jobId, sequence, GET_OBJECTS_LIMIT);
					} else {
						moveList = Utils.getDBInstance().getToRerunObjectsInfo(jobId, sequence, GET_OBJECTS_LIMIT);
					}
						
					long end = System.currentTimeMillis();
					logger.info("getToMoveObjectsInfo : {}ms, list size : {}", end - start, moveList.size());
					
					if (moveList.isEmpty() && sequence >= maxSequence) {
						break;
					}

					start = System.currentTimeMillis();
					for (MoveData moveData : moveList) {
						boolean skipCheck = moveData.isSkipCheck();
						if (skipCheck) {
							continue;
						}

						boolean isDelete = moveData.isDelete();		
						boolean isLatest = moveData.isLatest();

						// pass if delete and not latest for versioning		
						if (isVersioning) {
							if (isDelete) {
								if (isLatest) {		
									deletedList.add(moveData);
								} else {		
									logger.warn("Ignore this delete marker because it is not lastest. path={}, versionId={}", 
										moveData.getPath(),
										moveData.getVersionId());
								}
								continue;
							} else {
								if (isLatest) {
									latestList.add(moveData);
									continue;
								}
							}
						}

						String path = moveData.getPath();
						boolean isFile = moveData.isFile();
						long size = moveData.getSize();
						String versionId = moveData.getVersionId();

						String etag = moveData.getETag();
						String multipartInfo = moveData.getMultiPartInfo();
						if (multipartInfo != null && multipartInfo.compareTo("0") == 0) {
							multipartInfo = null;
						}

						logger.debug("path:{}, versionId:{}, size:{}", path, versionId, size);

						switch (retryMoveObject(path, isDelete, isLatest, isFile, versionId, etag, multipartInfo, size)) {
						case MOVE_SUCCESS:
							if (versionId != null && !versionId.isEmpty()) {
								logger.info("move success : {}:{}", path, versionId);
							} else {
								logger.info("move success : {}", path);
							}
							Utils.updateJobResult(jobId, true, path, versionId, size, isRerun);
							break;
						case MOVE_FAILURE:
							if (versionId != null && !versionId.isEmpty()) {
								logger.error("move failed : {}:{}", path, versionId);
							} else {
								logger.error("move failed : {}", path);
							}
							Utils.updateJobResult(jobId, false, path, versionId, size, isRerun);
							break;
						case DELETE_SUCCESS:
							if (versionId != null && !versionId.isEmpty()) {
								logger.info("delete success : {}:{}", path, versionId);
							} else {
								logger.info("delete success : {}", path);
							}
							Utils.getDBInstance().updateJobDeleted(jobId, size);
							break;
						case SKIP:
							if (versionId != null && !versionId.isEmpty()) {
								logger.info("skip : {}:{}", path, versionId);
							} else {
								logger.info("skip : {}", path);
							}
							Utils.updateJobSkipInfo(jobId, size);
							break;
						default:
							break;
						}
					}
					end = System.currentTimeMillis();
					logger.info("move time: {}ms, list size : {}", end - start, moveList.size());
					sequence += (threadCount * GET_OBJECTS_LIMIT);

					if (isVersioning) {
						if (latestList.size() >= GET_OBJECTS_LIMIT) {
							moveLatestObjects(false);
						}
						if (deletedList.size() >= GET_OBJECTS_LIMIT) {
							moveDeletedObjects(false);
						}
					}
				}

				if (isVersioning) {
					if (!latestList.isEmpty()) {
						moveLatestObjects(true);
					}
					if (!deletedList.isEmpty()) {
						moveDeletedObjects(true);
					}
				}

				logger.info("thread-{} finished", threadNumber);

				
			} catch (Exception e) {
				Utils.logging(logger, e);
			} finally {
				MDC.remove("logFileName");
			}
		}

		private void moveLatestObjects(boolean last) {
			for (Iterator<MoveData> iterator = latestList.iterator(); iterator.hasNext();) {
				MoveData jobInfo = iterator.next();
				boolean isDelete = jobInfo.isDelete();
				boolean isLatest = jobInfo.isLatest();
				String path = jobInfo.getPath();
				boolean isFile = jobInfo.isFile();
				long size = jobInfo.getSize();
				String versionId = jobInfo.getVersionId();
				String etag = jobInfo.getETag();
				String multipartInfo = jobInfo.getMultiPartInfo();
				if (multipartInfo != null && multipartInfo.compareTo("0") == 0) {
					multipartInfo = null;
				}

				logger.debug("path:{}, versionId:{}, size:{}", path, versionId, size);

				switch (retryMoveObject(path, isDelete, isLatest, isFile, versionId, etag, multipartInfo, size)) {
				case MOVE_SUCCESS:
					if (versionId != null && !versionId.isEmpty()) {
						logger.info("move success : {}:{}", path, versionId);
					} else {
						logger.info("move success : {}", path);
					}
					Utils.updateJobResult(jobId, true, path, versionId, size, isRerun);
					break;
				case MOVE_FAILURE:
					logger.error("move failed : {}", path);
					Utils.updateJobResult(jobId, false, path, versionId, size, isRerun);
					break;
				case DELETE_SUCCESS:
					Utils.getDBInstance().updateJobDeleted(jobId, size);
					break;
				case SKIP:
					Utils.updateJobSkipInfo(jobId, size);
					break;
				default:
					break;
				}

				iterator.remove();
				if (!last && (latestList.size() < GET_OBJECTS_LIMIT)) {
					break;
				} 
			}
		}

		private void moveDeletedObjects(boolean last) {
			for (Iterator<MoveData> iterator = deletedList.iterator(); iterator.hasNext();) {
				MoveData jobInfo = iterator.next();
				boolean isDelete = jobInfo.isDelete();
				boolean isLatest = jobInfo.isLatest();
				String path = jobInfo.getPath();
				boolean isFile = jobInfo.isFile();
				long size = jobInfo.getSize();
				String versionId = jobInfo.getVersionId();
				String etag = jobInfo.getETag();
				String multipartInfo = jobInfo.getMultiPartInfo();
				if (multipartInfo != null && multipartInfo.compareTo("0") == 0) {
					multipartInfo = null;
				}

				logger.debug("path:{}, versionId:{}, size:{}", path, versionId, size);


				switch (retryMoveObject(path, isDelete, isLatest, isFile, versionId, etag, multipartInfo, size)) {
				case MOVE_SUCCESS:
					Utils.updateJobResult(jobId, true, path, versionId, size, isRerun);
					break;
				case MOVE_FAILURE:
					logger.error("move failed : {}", path);
					Utils.updateJobResult(jobId, false, path, versionId, size, isRerun);
					break;
				case DELETE_SUCCESS:
					Utils.getDBInstance().updateJobDeleted(jobId, size);
					break;
				case SKIP:
					Utils.updateJobSkipInfo(jobId, size);
					break;
				default:
					break;
				}

				iterator.remove();
				if (!last && (deletedList.size() < GET_OBJECTS_LIMIT)) {
					break;
				} 
			}
		}
	}
	
	class ObjectMtimeComparator implements Comparator<HashMap<String, Object>> {

		@Override
		public int compare(HashMap<String, Object> o1, HashMap<String, Object> o2) {
			String mtime1 = (String) o1.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MTIME);
			String mtime2 = (String) o2.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MTIME);

			return mtime1.compareTo(mtime2);
		}
	}
}
