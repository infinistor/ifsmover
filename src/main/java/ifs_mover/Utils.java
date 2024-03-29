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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.VersionListing;
import com.google.common.base.CharMatcher;

import ifs_mover.db.MariaDB;
import ifs_mover.db.MoverDB;
import ifs_mover.db.SqliteDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

	private static MoverDB moverDB;
    public final static int RETRY_COUNT = 3;

	// private static final List<Map<String, String>>movedObjectList = new ArrayList<Map<String, String>>();
	// private static final List<Map<String, Long>>movedJobList = new ArrayList<Map<String, Long>>();
	// private static final List<Map<String, String>>failedObjectList = new ArrayList<Map<String, String>>();
	// private static final List<Map<String, Long>>failedJobList = new ArrayList<Map<String, Long>>();
	
	// public static List<Map<String, String>> getMovedObjectList() {
	// 	return movedObjectList;
	// }

	// public static List<Map<String, Long>> getMovedJobList() {
	// 	return movedJobList;
	// }

	// public static List<Map<String, String>> getFailedObjectList() {
	// 	return failedObjectList;
	// }

	// public static List<Map<String, Long>> getFailedJobList() {
	// 	return failedJobList;
	// }

    private static final CharMatcher VALID_BUCKET_CHAR =
			CharMatcher.inRange('a', 'z')
			.or(CharMatcher.inRange('0', '9'))
			.or(CharMatcher.is('-'))
			.or(CharMatcher.is('.'));
            
    public static boolean isValidBucketName(String bucketName) {
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

    public static String getS3BucketName(String bucketName) {
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

    public static void insertMoveObject(String jobId, boolean isFile, String mTime, long size, String path, String etag, String tag) {
        for (int i = 0; i < RETRY_COUNT; i++) {
            if (getDBInstance().insertMoveObject(jobId, isFile, mTime, size, path, etag, tag)) {
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

	public static long insertMoveObject(String jobId, ObjectListing objectListing) {
		long totalSize = 0;
        for (int i = 0; i < RETRY_COUNT; i++) {
            if ((totalSize = getDBInstance().insertMoveObject(jobId, objectListing)) > 0) {
				return totalSize;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
        }
        logger.error("failed insertMoveObject. path={}", objectListing.toString());
		return totalSize;
    }

	public static long insertMoveObject(String jobId, List<MoveData> list) {
		long totalSize = 0;
        for (int i = 0; i < RETRY_COUNT; i++) {
            if ((totalSize = getDBInstance().insertMoveObject(jobId, list)) > 0) {
				return totalSize;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
        }
        logger.error("failed insertMoveObject. path={}", list.toString());
		return totalSize;
	}

	public static void insertMoveObjectVersion(String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().insertMoveObjectVersioning(jobId, isFile, mTime, size, path, versionId, etag, multipartInfo, tag, isDelete, isLatest)) {
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

	public static long insertRerunObject(String jobId, ObjectListing objectListing) {
		long totalSize = -1;
        for (int i = 0; i < RETRY_COUNT; i++) {
            if ((totalSize = getDBInstance().insertRerunObject(jobId, objectListing)) > 0) {
				return totalSize;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
        }
        logger.error("failed insertMoveObject. path={}", objectListing.toString());
		return totalSize;
    }

	public static long insertMoveObjectVersion(String jobId, VersionListing versionListing) {
		long totalSize = -1;
        for (int i = 0; i < RETRY_COUNT; i++) {
            if ((totalSize = getDBInstance().insertMoveObjectVersioning(jobId, versionListing)) > 0) {
				return totalSize;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
        }
        logger.error("failed insertMoveObject. path={}", versionListing.toString());
		return totalSize;
	}

	public static long insertRerunObjectVersion(String jobId, VersionListing versionListing) {
		long totalSize = -1;
        for (int i = 0; i < RETRY_COUNT; i++) {
            if ((totalSize = getDBInstance().insertRerunObjectVersioning(jobId, versionListing)) > 0) {
				return totalSize;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
        }
        logger.error("failed insertMoveObject. path={}", versionListing.toString());
		return totalSize;
	}

    public static void updateJobInfo(String jobId, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobInfo(jobId, size)) {
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

	public static void updateJobInfo(String jobId, int count, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobInfo(jobId, count, size)) {
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

    public static void insertRerunMoveObject(String jobId, boolean isFile, String mTime, long size, String path, String etag, String multipartInfo, String tag) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().insertRerunMoveObject(jobId, isFile, mTime, size, path, etag, multipartInfo, tag)) {
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

	public static void insertRerunMoveObjectVersion(String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().insertRerunMoveObjectVersion(jobId, isFile, mTime, size, path, versionId, etag, multipartInfo, tag, isDelete, isLatest)) {
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

    public static void updateJobRerunInfo(String jobId, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobRerunInfo(jobId, size)) {
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

	public static void updateJobRerunInfo(String jobId, int count, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobRerunInfo(jobId, count, size)) {
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

    public static void updateRerunSkipObject(String jobId, String path) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateRerunSkipObject(jobId, path)) {
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

    public static void updateJobRerunSkipInfo(String jobId, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobRerunSkipInfo(jobId, size)) {
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

	public static void updateJobSkipInfo(String jobId, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobSkipInfo(jobId, size)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateJobSkipInfo. size={}", size);
	}

    public static void updateToMoveObject(String jobId, String mTime, long size, String path) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateToMoveObject(jobId, mTime, size, path)) {
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

	public static void updateObjectMove(String jobId, String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectMove(jobId, path, versionId)) {
				return;
			}
		}

		logger.error("failed updateObjectMove. path={}", path);
	}

	public static void updateObjectRerun(String jobId, String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectRerun(jobId, path, versionId)) {
				return;
			}
		}

		logger.error("failed updateObjectMove. path={}", path);
	}

	public static void updateToMoveObjectVersion(String jobId, String mTime, long size, String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateToMoveObjectVersion(jobId, mTime, size, path, versionId)) {
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

	public static void updateRerunSkipObjectVersion(String jobId, String path, String versionId, boolean isLatest) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateRerunSkipObjectVersion(jobId, path, versionId, isLatest)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateRerunSkipObjectVersion. path={}, versionId={}", path, versionId);
	}

	public static void updateSkipObject(String jobId, String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateSkipObject(jobId, path, versionId)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed updateSkipObject. path={}, versionId={}", path, versionId);
	}

	public static void insertTargetObject(String jobId, String path, String versionId, long size, String etag) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().insertTargetObject(jobId, path, versionId, size, etag)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}

		logger.error("failed insertTargetObject. path={}", path);
	}

	public static boolean compareObject(String jobId, String path, long size, String etg) {
		return getDBInstance().compareObject(jobId, path, size, etg);
	}

	public static void logging(Logger log, Exception e) {
		log.error(e.getMessage());
		for (StackTraceElement k : e.getStackTrace() ) {
			log.error(k.toString());
		}
	}

	// private static void addMovedObjectList(String path, String versionId) {
	// 	Map<String, String> map = new HashMap<String, String>();
	// 	map.put("path", path);
	// 	map.put("versionId", versionId);

	// 	movedObjectList.add(map);
	// }

	// private static void addMovedJobList(long size) {
	// 	Map<String, Long> map = new HashMap<String, Long>();
	// 	map.put("size", Long.valueOf(size));

	// 	movedJobList.add(map);
	// }

	// private static void addFailedObjectList(String path, String versionId) {
	// 	Map<String, String> map = new HashMap<String, String>();
	// 	map.put("path", path);
	// 	map.put("versionId", versionId);

	// 	failedObjectList.add(map);
	// }

	// private static void addFailedJobList(long size) {
	// 	Map<String, Long> map = new HashMap<String, Long>();
	// 	map.put("size", Long.valueOf(size));

	// 	failedJobList.add(map);
	// }

	public static void updateJobResult(String jobId, boolean success, String path, String versionId, long size, boolean isRerun) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobResult(jobId, success, path, versionId, size, isRerun)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}

	public static void updateJobMoved(String jobId, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobMoved(jobId, size)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed update move info to Job table(size={})", size);
		// addMovedJobList(size);
	}

	public static void updateJobMoved(String jobId, int count, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobMoved(jobId, count, size)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed update move info to Job table(size={})", size);
		// addMovedJobList(size);
	}

	public static void updateObjectMoveEvent(String jobId, String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectMoveComplete(jobId, path, versionId)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed update move info. {}:{}", path, versionId);
		// addMovedObjectList(path, versionId);
	}

	public static void updateObjectMoveEvent(String jobId, List<HashMap<String, Object>> list) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectMoveComplete(jobId, list)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed update move info. {}:{}", path, versionId);
		// addMovedObjectList(path, versionId);
	}
	
	public static void updateObjectRerunEvent(String jobId, String path, String versionId) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectRerunComplete(jobId, path, versionId)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed update move info. {}:{}", path, versionId);
		// addMovedObjectList(path, versionId);
	}

	public static void updateObjectRerunEvent(String jobId, List<HashMap<String, Object>> list) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectRerunComplete(jobId, list)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed update move info. {}:{}", path, versionId);
		// addMovedObjectList(path, versionId);
	}

	public static void updateObjectVersionMoveEventFailed(String jobId, String path, String versionId, String errorCode, String errorDesc) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectMoveEventFailed(jobId, path, versionId, "", "retry failure")) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed UpdateObjectVersionMoveEventFailed. {}:{}", path, versionId);
		// addFailedObjectList(path, versionId);
	}

	public static void updateObjectVersionMoveEventFailed(String jobId, List<HashMap<String, Object>> list) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectMoveEventFailed(jobId, list)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed UpdateObjectVersionMoveEventFailed. {}:{}", path, versionId);
		// addFailedObjectList(path, versionId);
	}

	public static void updateObjectRerunEventFailed(String jobId, List<HashMap<String, Object>> list) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateObjectRerunEventFailed(jobId, list)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed UpdateObjectVersionMoveEventFailed. {}:{}", path, versionId);
		// addFailedObjectList(path, versionId);
	}

	public static void updateJobFailedInfo(String jobId, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobFailedInfo(jobId, size)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed UpdateJobFailedInfo. {}", size);
		// addFailedJobList(size);
	}

	public static void updateJobFailedInfo(String jobId, int count, long size) {
		for (int i = 0; i < RETRY_COUNT; i++) {
			if (getDBInstance().updateJobFailedInfo(jobId, count, size)) {
				return;
			} else {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
		// logger.error("failed UpdateJobFailedInfo. {}", size);
		// addFailedJobList(size);
	}

	public static MoverDB getDBInstance() {
		if (moverDB == null) {
			if (MoverConfig.getInstance().getDBRepository().equalsIgnoreCase(MoverConfig.MARIADB)) {
				moverDB = MariaDB.getInstance();
			} else if (MoverConfig.getInstance().getDBRepository().equalsIgnoreCase(MoverConfig.SQLITEDB)) {
				moverDB = SqliteDB.getInstance();
			} else {
				logger.error("undefined db repository.");
				moverDB = null;
			}
		}
		
		return moverDB;
	}

	public static boolean isExistMoveTable(String db, String jobId) {
		if (getDBInstance().isExistMoveTable(db, jobId)) {
			return true;
		}
		return false;
	}

	public static boolean isExistRerunTable(String db, String jobId) {
		if (getDBInstance().isExistRerunTable(db, jobId)) {
			return true;
		}
		return false;
	}
}
