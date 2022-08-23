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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.Tag;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectMover {
	private static final Logger logger = LoggerFactory.getLogger(ObjectMover.class);
	
	private Config sourceConfig;
	private Config targetConfig;
	private int threadCount;
	private String type;
	private boolean isRerun;
	private boolean isVersioning;
	private String jobId;
	private Repository sourceRepository;
	private IfsS3 targetRepository;
	private boolean targetVersioning;

	private final int GET_OBJECTS_LIMIT = 100;
	private final String NO_SUCH_KEY = "NoSuchKey";
	private final String NOT_FOUND = "Not Found";
	private final long MEGA_BYTES = 1024 * 1024;
	private final long GIGA_BYTES = 1024 * 1024 * 1024;
	private final long MAX_MULTIPART_SIZE = 3 * GIGA_BYTES;
	
	public static class Builder {
		private Config sourceConfig;
		private Config targetConfig;
		private int threadCount;
		private String type;
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
		isRerun = builder.isRerun;
		jobId = builder.jobId;

		RepositoryFactory factory = new RepositoryFactory();
		sourceRepository = factory.getSourceRepository(type, jobId);
		sourceRepository.setConfig(sourceConfig, true);
		targetRepository = factory.getTargetRepository(jobId);
		targetRepository.setConfig(targetConfig, false);

		logger.info("thread count: {}", threadCount);
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
		if (targetConfig.getVersoning() == null || targetConfig.getVersoning().isEmpty()) {
			targetVersioning = true;
		} else if (targetConfig.getVersoning().compareToIgnoreCase("OFF") == 0) {
			targetVersioning = false;
		} else {
			targetVersioning = true;
		}

		// check source bucket versioning
		if (sourceRepository.isVersioning()) {
			// check target bucket versioning
			if (!targetRepository.isVersioning()) {
				if (targetVersioning) {
					isVersioning = true;
					targetRepository.setVersioning();
				} else {
					isVersioning = false;
				}
			} else {
				isVersioning = true;
				targetVersioning = true;
			}
		} else {
			isVersioning = false;
			targetVersioning = false;
		}

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

		sourceRepository.makeObjectList(isRerun, targetVersioning);
		if (isRerun) {
			Utils.getDBInstance().deleteCheckObjects(jobId);
		}
		targetRepository.makeTargetObjectList(targetVersioning);
	}

	public void moveObjects() {		
		String finalPath = "";
		String path = "";
		String versionId = "";
		boolean isDoneDistribute = false;
		long sequence = 0;

		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		List<HashMap<String, Object>> list = null;
		List<HashMap<String, Object>> jobList = null;

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
						targetRepository.putObject(false, targetConfig.getBucket(), prefixFinal, data, 0L);
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
	
			long maxSequence = Utils.getDBInstance().getMaxSequence(jobId);
			int state = 0;
			while (true) {
				if (!isDoneDistribute) {
					list = Utils.getDBInstance().getToMoveObjectsInfo(jobId, sequence, GET_OBJECTS_LIMIT);

					if (list == null && sequence >= maxSequence) {
						if (jobList != null && !jobList.isEmpty()) {
							Mover mover = new Mover(isRerun, jobList);
							executor.execute(mover);
						}
						isDoneDistribute = true;
						executor.shutdown();
						continue;
					}
					sequence += GET_OBJECTS_LIMIT;
					if (list != null && !list.isEmpty()) {
						for (HashMap<String, Object> jobInfo : list) {
							state = (int) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE);
							if (state != 1) {
								continue;
							}

							path = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH);
							versionId = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID);
							logger.info("path : {}, versionId : {}", path, versionId);
							Utils.updateObjectMove(jobId, path, versionId);

							if (path.compareTo(finalPath) != 0) {
								finalPath = path;
								if (jobList == null) {
									jobList = new ArrayList<HashMap<String, Object>>();
									jobList.add(jobInfo);
								} else {
									Mover mover = new Mover(isRerun, jobList);
									executor.execute(mover);
									jobList = new ArrayList<HashMap<String, Object>>();
									jobList.add(jobInfo);
								}
							} else {
								if (jobList == null) {
									jobList = new ArrayList<HashMap<String, Object>>();
								}
								jobList.add(jobInfo);
							}
						}
					}
				} 
				
				if (isDoneDistribute && executor.isTerminated()) {
					deleteMove();
	
					if (isDoneDistribute && executor.isTerminated()) {
						for (Map<String, String> movedObject : Utils.getMovedObjectList()) {
							path = movedObject.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH);
							versionId = movedObject.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID);
		
							Utils.getDBInstance().updateObjectMoveComplete(jobId, path, versionId);
						}
		
						for (Map<String, Long> movedJob : Utils.getMovedJobList()) {
							long size = movedJob.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_SIZE).longValue();
		
							Utils.getDBInstance().updateJobMoved(jobId, size);
						}
		
						for (Map<String, String> failedObject : Utils.getFailedObjectList()) {
							path = failedObject.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH);
							versionId = failedObject.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID);
		
							Utils.getDBInstance().updateObjectMoveEventFailed(jobId, path, versionId, "", "retry failure");
						}
		
						for (Map<String, Long> failedJob : Utils.getFailedJobList()) {
							long size = failedJob.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_SIZE).longValue();
							Utils.getDBInstance().updateJobFailedInfo(jobId, size);
						}
	
						if (isVersioning && targetVersioning) {
							try {
								targetRepository.setBucketVersioning(sourceRepository.getVersioningStatus());
							} catch (AmazonServiceException ase) {
								logger.warn("{} {}", ase.getErrorCode(), ase.getErrorMessage());
							} catch (SdkClientException  ace) {
								logger.warn("{}", ace.getMessage());
							}
						}
						logger.info("End of moving jobs");
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
		} catch (Exception e) {
			Utils.logging(logger, e);
		}
	}

	private void deleteMove() {
		String finalPath = "";
		String path = "";
		String versionId = "";
		boolean isDoneDistribute = false;
		long sequence = 0;
		long limit = 100;

		ExecutorService executor = Executors.newCachedThreadPool(); // .newFixedThreadPool(threadCount);
		List<HashMap<String, Object>> list = null;
		List<HashMap<String, Object>> jobList = null;

		sequence = 0;
		isDoneDistribute = false;

		long maxSequence = Utils.getDBInstance().getMaxSequence(jobId);

		int state = 0;
		while (true) {
			if (!isDoneDistribute) {
				list = Utils.getDBInstance().getToDeleteObjectsInfo(jobId, sequence, limit);
				if (list == null && sequence >= maxSequence) {
					if (jobList != null && !jobList.isEmpty()) {
						Mover mover = new Mover(isRerun, jobList);
						executor.execute(mover);
					}
					isDoneDistribute = true;
					executor.shutdown();
					continue;
				}
				sequence += limit;

				if (list != null && !list.isEmpty()) {
					for (HashMap<String, Object> jobInfo : list) {
						state = (int) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE);
						if (state != 1) {
							continue;
						}
						path = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH);
						versionId = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID);
	
						Utils.updateObjectMove(jobId, path, versionId);
						
						if (path.compareTo(finalPath) != 0) {
							finalPath = path;
							if (jobList == null) {
								jobList = new ArrayList<HashMap<String, Object>>();
								jobList.add(jobInfo);
							} else {
								Mover mover = new Mover(isRerun, jobList);
								executor.execute(mover);
								jobList = new ArrayList<HashMap<String, Object>>();
								jobList.add(jobInfo);
							}
						} else {
							if (jobList == null) {
								jobList = new ArrayList<HashMap<String, Object>>();
							}
							jobList.add(jobInfo);
						}
					}
				}
			}

			if (isDoneDistribute && executor.isTerminated()) {
				return;
			}
		}
	}
	

	class Mover implements Runnable {
		final Logger logger = LoggerFactory.getLogger(Mover.class);
		private boolean isRerun;
		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
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
		private boolean skipCheck;
		private boolean latestSkipCheck;

		private boolean isFault;
		private boolean isDoneLatest;

		private boolean isTargetSkip;
		
		public Mover(boolean isRerun, List<HashMap<String, Object>> list) {
			this.isRerun = isRerun;
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
		
		private boolean moveObject(String path, boolean isDelete, boolean isFile, String versionId, String etag, String multipartInfo, String tag, long size, boolean skipCheck) {
			String sourcePath = null;
			String targetPath = null;
			String sourceBucket = null;
			String targetBucket = null;
			
			Map<String, String> tagMap = null;
			List<Tag> tagSet = new ArrayList<Tag>();
			
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

			if (tag != null && !tag.isEmpty()) {
				try {
					tagMap = new ObjectMapper().readValue(tag, Map.class);
					for (String key : tagMap.keySet()) {
						Tag s3tag = new Tag(key, tagMap.get(key));
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

			if (versionId != null && versionId.compareTo("0") == 0) {
				versionId = null;
			}

			try {
				if (isDelete) {
					if (isRerun) {
						if (isLatest && !skipCheck) {
							targetRepository.deleteObject(targetBucket, targetPath, null);
							logger.info("delete success : {}", sourcePath);
						} else {
							// Ignore this delete marker because it is not lastest
							logger.warn("Ignore this delete marker because it is not lastest. path={}, versionId={}", path, versionId);
						}
					} else {
						if (isLatest) {
							targetRepository.deleteObject(targetBucket, targetPath, null);
							logger.info("delete success : {}", sourcePath);
						} else {
							// Ignore this delete marker because it is not lastest
							logger.warn("Ignore this delete marker because it is not lastest. path={}, versionId={}", path, versionId);
						}
					}
					Utils.getDBInstance().updateDeleteMarker(jobId, path, versionId);
				} else {
					// Check if it is the same as target object
					if (targetRepository.isTargetSync()) {
						if (targetRepository.getTargetSyncMode() == SyncMode.ETAG && type.equalsIgnoreCase(Repository.S3)) { // check ETAG
							if (Utils.getDBInstance().compareObject(jobId, path, etag)) {
								logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
								Utils.updateSkipObject(jobId, path, versionId);
								Utils.updateJobSkipInfo(jobId, size);
								this.isTargetSkip = true;
								return true;
							}
						} else if (targetRepository.getTargetSyncMode() == SyncMode.SIZE) {	// check SIZE
							if (type.equalsIgnoreCase(Repository.S3)) {
								if (Utils.getDBInstance().compareObject(jobId, path, size)) {
									logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
									Utils.updateSkipObject(jobId, path, versionId);
									Utils.updateJobSkipInfo(jobId, size);
									this.isTargetSkip = true;
									return true;
								}
							} else if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
								logger.debug("path : {}", targetPath);
								if (Utils.getDBInstance().compareObject(jobId, targetPath, size)) {
									logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
									Utils.updateSkipObject(jobId, path, versionId);
									Utils.updateJobSkipInfo(jobId, size);
									this.isTargetSkip = true;
									return true;
								}
							}
						} else if (targetRepository.getTargetSyncMode() == SyncMode.EXIST) { // check EXIST
							if (type.equalsIgnoreCase(Repository.S3)) {
								if (Utils.getDBInstance().isExistObject(jobId, path)) {
									logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
									Utils.updateSkipObject(jobId, path, versionId);
									Utils.updateJobSkipInfo(jobId, size);
									this.isTargetSkip = true;
									return true;
								}
							} else if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
								logger.debug("path : {}", targetPath);
								if (Utils.getDBInstance().isExistObject(jobId, targetPath)) {
									logger.warn("Ignored because it is the same as the target object. path : {}, versionId : {}, size : {}, etag : {}", path, versionId, size, etag);
									Utils.updateSkipObject(jobId, path, versionId);
									Utils.updateJobSkipInfo(jobId, size);
									this.isTargetSkip = true;
									return true;
								}
							}
						}
					}

					if (isFile) {
						// check rerun delete
						if (isRerun && !skipCheck) {
							if (isVersioning) {
								logger.warn("Ignore this object(skip check is 0), path : {}, versionId : {}", path, versionId);
								Utils.getDBInstance().updateDeleteMarker(jobId, path, versionId);
								// In versioning, delete from db if not lastest
								// if (!isLatest) {
								// 	Utils.getDBInstance().deleteObjects(jobId, path, versionId);
								// }
							} else {
								targetRepository.deleteObject(targetBucket, targetPath, null);
								Utils.getDBInstance().updateJobDeleted(jobId, size);
								Utils.getDBInstance().deleteObjects(jobId, path, versionId);
								logger.info("delete success : {}", sourcePath);
							}
							return true;
						}

						if (multipartInfo != null && !multipartInfo.isEmpty()) {
							// for swift large file (more than 5G)
							String uploadId = targetRepository.startMultipart(targetBucket, targetPath, null);
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
								data = sourceRepository.getObject(multiPath[0], partPath, null);
								if (data != null) {
									String partETag = targetRepository.uploadPart(targetBucket, targetPath, uploadId, data.getInputStream(), partNumber, data.getSize());
									partList.add(new PartETag(partNumber, partETag));
									data.getInputStream().close();
								}
							} while (data != null);

							targetRepository.completeMultipart(targetBucket, targetPath, uploadId, partList);
							if (tagSet.size() > 0) {
								targetRepository.setTagging(targetBucket, targetPath, tagSet);
							}
							logger.info("move success : {}", path);
						} else if ((!type.equalsIgnoreCase(Repository.IFS_FILE) && size > MAX_MULTIPART_SIZE)
									|| (!type.equalsIgnoreCase(Repository.IFS_FILE) && (moveSize != 0 && size > moveSize))) {
							// send multipart
							if (versionId != null && !versionId.isEmpty()) {
								logger.debug("send multipart : {}:{}, size {}", path, versionId, size);
							} else {
								logger.debug("send multipart : {}, size {}", path, size);
							}
							long limitSize = 0L;
							if (moveSize == 0) {
								limitSize = 100 * MEGA_BYTES;
							} else {
								limitSize = moveSize;
							}

							ObjectMetadata objectMetadata = sourceRepository.getMetadata(sourceBucket, sourcePath, versionId);
							String uploadId = targetRepository.startMultipart(targetBucket, targetPath, objectMetadata);
							List<PartETag> partList = new ArrayList<PartETag>();
							int partNumber = 1;

							for (long i = 0; i < size; i += limitSize, partNumber++) {
								long start = i;
								long end = i + limitSize - 1;
								if (end >= size) {
									end = size - 1;
								}

								ObjectData data = sourceRepository.getObject(sourceBucket, sourcePath, versionId, start, end);

								if (data != null) {
									String partETag = targetRepository.uploadPart(targetBucket, targetPath, uploadId, data.getInputStream(), partNumber, data.getSize());
									partList.add(new PartETag(partNumber, partETag));
									data.close();
									logger.info("{} - move part : {}, size : {}", path, partNumber, data.getSize());
								}
							}

							targetRepository.completeMultipart(targetBucket, targetPath, uploadId, partList);

							if (tagSet.size() > 0) {
								targetRepository.setTagging(targetBucket, targetPath, tagSet);
							}

							if (versionId != null && !versionId.isEmpty()) {
								logger.info("move success : {}:{}", path, versionId);
							} else {
								logger.info("move success : {}", path);
							}
						} else {
							ObjectData data = null;
							String s3ETag = null;
							if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
								data = sourceRepository.getObject(sourcePath);
								s3ETag = targetRepository.putObject(isFile, targetBucket, targetPath, data, data.getSize());
								data.close();
							} else {
								if (size > 0) {
									data = sourceRepository.getObject(sourceBucket, sourcePath, versionId, 0);
									if (data == null) {
										logger.warn("not found : {} {}", sourceBucket, sourcePath);
										return false;
									}
									s3ETag = targetRepository.putObject(isFile, targetBucket, targetPath, data, data.getSize());
									data.close();
								} else {
									ObjectMetadata meta = sourceRepository.getMetadata(sourceBucket, sourcePath, versionId);
									InputStream is = new ByteArrayInputStream(new byte[0]);
									targetRepository.putObject(targetBucket, targetPath, is, meta); 
								}
							}

							if (tagSet.size() > 0) {
								targetRepository.setTagging(targetBucket, targetPath, tagSet);
							}

							if (!type.equalsIgnoreCase(Repository.IFS_FILE) && isFile && size > 0) {
								if (etag.equals(s3ETag) || etag.contains("-")) {
									if (versionId != null && !versionId.isEmpty()) {
										logger.info("move success : {}:{}", path, versionId);
									} else {
										logger.info("move success : {}", path);
									}
								} else {
									// The etag is different, but we have already putObjected, so let's just leave a log.
									logger.warn("{}:{} -- The etags are different. source : {}, target : {}", path, versionId, etag, s3ETag);
									return true;
								}
							} else {
								if (versionId != null && !versionId.isEmpty()) {
									logger.info("move success : {}:{}", path, versionId);
								} else {
									logger.info("move success : {}", path);
								}
							}
						}
					} else {	// move dir
						// check rerun delete
						if (isRerun && !skipCheck) {
							if (isVersioning) {
								logger.warn("Ignore this object(skip check is 0), path : {}, versionId : {}", path, versionId);
								Utils.getDBInstance().updateDeleteMarker(jobId, path, versionId);
							} else {
								targetRepository.deleteObject(targetBucket, targetPath, null);
								Utils.getDBInstance().updateJobDeleted(jobId, size);
								Utils.getDBInstance().deleteObjects(jobId, path, versionId);
								logger.info("delete success : {}", sourcePath);
							}
							return true;
						}

						ObjectData data;
						if (type.equalsIgnoreCase(Repository.IFS_FILE)) {
							ObjectMetadata meta = new ObjectMetadata();
							meta.setContentLength(0);
							InputStream is = new ByteArrayInputStream(new byte[0]);
							data = new ObjectData(meta, is);
							targetRepository.putObject(isFile, targetBucket, targetPath, data, 0L);
							is.close();
						} else {
							if (versionId != null && versionId.compareToIgnoreCase("null") == 0) {
								ObjectMetadata meta = sourceRepository.getMetadata(sourceBucket, sourcePath, versionId);
								InputStream is = new ByteArrayInputStream(new byte[0]);
								// data = sourceRepository.getObject(sourceBucket, sourcePath, versionId);
								// data = sourceRepository.getObject(sourceBucket, sourcePath, null);
								// if (data == null) {
								// 	logger.warn("not found : {} {}", sourceBucket, sourcePath);
								// 	return false;
								// }
								// targetRepository.putObject(isFile, targetBucket, targetPath, data, 0L);
								targetRepository.putObject(targetBucket, targetPath, is, meta);
								is.close();
								if (tagSet.size() > 0) {
									targetRepository.setTagging(targetBucket, targetPath, tagSet);
								}
							} else {
								logger.info("Ignore this object because Because it is a directory with versionid, path : {}, versionId : {}", path, versionId);
							}
						}

						if (versionId != null && !versionId.isEmpty()) {
							logger.info("move success : {}:{}", path, versionId);
						} else {
							logger.info("move success : {}", path);
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
				return false;
			} catch (AmazonClientException ace) {
				logger.warn("{} {}", path, ace.getMessage());
				// Ignore 'etag' even if it's different after gettingObject in S3
				// try {
				// 	ObjectMetadata objectMetadata = sourceRepository.getMetadata(sourceBucket, sourcePath, versionId);
				// 	String command = null;
				// 	File file = null;
				// 	if (versionId != null && !versionId.isEmpty() || versionId.compareToIgnoreCase("null") == 0) {
				// 		command = "aws s3api get-object --endpoint-url http://" + sourceConfig.getEndPoint() + " --bucket " + sourceBucket + " --key " + sourcePath + " ./download/" + jobId + "/" + sourcePath;
				// 		file = new File("./download/" + jobId + "/" + sourcePath);
				// 	} else {
				// 		command = "aws s3api get-object --endpoint-url http://" + sourceConfig.getEndPoint() + " --bucket " + sourceBucket + " --key " + sourcePath + " --version-id " + versionId + " ./download/" + jobId + "/" + sourcePath + "_" + versionId;
				// 		file = new File("./download/" + jobId + "/" + sourcePath + "_" + versionId);
				// 	}

				// 	com.google.common.io.Files.createParentDirs(file);
					
				// 	logger.info("command : {}", command);
				// 	Process process = Runtime.getRuntime().exec(command);
				// 	BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
				// 	String line = null;

				// 	while ((line = reader.readLine()) != null) {
				// 		logger.info("{}", line);
				// 	}
					
				// 	process.waitFor();
				// 	logger.info("saved file ...");
				// 	FileInputStream fis = new FileInputStream(file);

				// 	targetRepository.putObject(targetBucket, targetPath, fis, objectMetadata);
				// 	fis.close();
				// 	file.delete();

				// 	logger.info("delete file ...");

				// 	if (tagSet.size() > 0) {
				// 		targetRepository.setTagging(targetBucket, targetPath, tagSet);
				// 	}

				// 	if (versionId != null && !versionId.isEmpty()) {
				// 		logger.info("move success : {}:{}", path, versionId);
				// 	} else {
				// 		logger.info("move success : {}", path);
				// 	}

				// 	return true;
				// } catch (Exception e) {
				// 	Utils.logging(logger, e);
				// }

				return false;
			} catch (Exception e) {
				Utils.logging(logger, e);
				return false;
			}

			return true;
		}
	
		private void retryMoveObject(String path, boolean isDelete, boolean isFile, String versionId, String etag, String multipartInfo, String tag, long size, boolean skipCheck) {
			for (int i = 0; i < Utils.RETRY_COUNT; i++) {
				if (moveObject(path, isDelete, isFile, versionId, etag, multipartInfo, tag, size, skipCheck)) {
					return;
				}
			}
			
			isFault = true;
		}

		@Override
		public void run() {
			try {
				MDC.clear();
				MDC.put("logFileName", "ifs_mover." + jobId + ".log");
				isFault = false;
				isDoneLatest = true;

				if (list.size() > 0) {
					logger.debug("before sort ...");
					for (HashMap<String, Object> jobInfo : list) {
						String path = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH);
						String versionId = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID);
						String mtime = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MTIME);
						logger.debug("path:{}, versionId:{}, mtime:{}", path, versionId, mtime);
					}
					
					Collections.sort(list, new ObjectMtimeComparator());
					logger.debug("after sort ...");
					for (HashMap<String, Object> jobInfo : list) {
						String path = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH);
						String versionId = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID);
						String mtime = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MTIME);
						logger.debug("path:{}, versionId:{}, mtime:{}", path, versionId, mtime);
					}
				}

				for (HashMap<String, Object> jobInfo : list) {
					path = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH);
					isFile = (byte) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISFILE) == 1; 
					size = (long) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_SIZE);
					versionId = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID);

					etag = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ETAG);
					multipartInfo = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MULTIPART_INFO);
					if (multipartInfo != null && multipartInfo.compareTo("0") == 0) {
						multipartInfo = null;
					}

					tag = (String) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_TAG);

					if (tag != null && tag.compareTo("0") == 0) {
						tag = null;
					}
					isDelete = (byte) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISDELETE) == 1;
					isLatest = (byte) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISLATEST) == 1;
					skipCheck = (byte) jobInfo.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_SKIP_CHECK) == 1;

					// Latest should be performed at the end.
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
						latestSkipCheck = skipCheck;
						continue;
					}

					isTargetSkip = false;
					retryMoveObject(path, isDelete, isFile, versionId, etag, multipartInfo, tag, size, skipCheck);
					if (isTargetSkip) {
						continue;
					}

					if (isRerun && !skipCheck) {
						continue;
					}
					
					if (isFault) {
						logger.error("move failed : {}", path);
						Utils.updateObjectVersionMoveEventFailed(jobId, path, versionId, "", "retry failure");
						Utils.updateJobFailedInfo(jobId, size);
					} else {
						Utils.updateJobMoved(jobId, size);
						Utils.updateObjectMoveEvent(jobId, path, versionId);
					}
					isFault = false;
				}

				if (!isDoneLatest) {
					// Latest should be performed at the end.
					isTargetSkip = false;
					retryMoveObject(latestPath, latestIsDelete, latestIsFile, latestVersionId, latestETag, latestMultipartInfo, latestTag, size, latestSkipCheck);

					if (!isTargetSkip && !(isRerun && !skipCheck)) {
						if (isFault) {
							logger.error("move failed : {}", latestPath);
							Utils.updateObjectVersionMoveEventFailed(jobId, latestPath, latestVersionId, "", "retry failure");
							Utils.updateJobFailedInfo(jobId, latestSize);
						} else {
							Utils.updateJobMoved(jobId, latestSize);			
							Utils.updateObjectMoveEvent(jobId, latestPath, latestVersionId);
						}
					}
				}
				MDC.remove("logFileName");
			} catch (Exception e) {
				Utils.logging(logger, e);
			}
		}
	}

	class ObjectMtimeComparator implements Comparator<HashMap<String, Object>> {

		@Override
		public int compare(HashMap<String, Object> o1, HashMap<String, Object> o2) {
			// TODO Auto-generated method stub
			String mtime1 = (String) o1.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MTIME);
			String mtime2 = (String) o2.get(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MTIME);

			return mtime1.compareTo(mtime2);
		}
	}
}
