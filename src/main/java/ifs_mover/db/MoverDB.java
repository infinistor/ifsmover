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
package ifs_mover.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ifs_mover.Config;
import ifs_mover.IMOptions;

public interface MoverDB {
    public static final String MOVE_OBJECTS_TABLE_COLUMN_PATH = "path";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_ISFILE = "isfile";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_SIZE = "size";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_VERSIONID = "version_id";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_ETAG = "etag";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_MULTIPART_INFO = "multipart_info";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_TAG = "tag";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_ISDELETE = "isdelete";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_ISLATEST = "islatest";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE = "object_state";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_MTIME = "mtime";
	public static final String MOVE_OBJECTS_TABLE_COLUMN_SKIP_CHECK = "skip_check";
	
	public static final String JOB_TABLE_COLUMN_JOB_ID = "job_id";
	public static final String JOB_TABLE_COLUMN_JOB_STATE = "job_state";
	public static final String JOB_TABLE_COLUMN_PID = "pid";
	public static final String JOB_TABLE_COLUMN_JOB_TYPE = "job_type";
	public static final String JOB_TABLE_COLUMN_SOURCE_POINT = "source_point";
	public static final String JOB_TABLE_COLUMN_TARGET_POINT = "target_point";
	public static final String JOB_TABLE_COLUMN_OBJECTS_COUNT = "objects_count";
	public static final String JOB_TABLE_COLUMN_OBJECTS_SIZE = "objects_size";
	public static final String JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT = "moved_objects_count";
	public static final String JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE = "moved_objects_size";
	public static final String JOB_TABLE_COLUMN_FAILED_COUNT = "failed_count";
	public static final String JOB_TABLE_COLUMN_FAILED_SIZE = "failed_size";
	public static final String JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT = "skip_objects_count";
	public static final String JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE = "skip_objects_size";
	public static final String JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT = "delete_objects_count";
	public static final String JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE = "delete_objects_size";
	public static final String JOB_TABLE_COLUMN_START = "start";
	public static final String JOB_TABLE_COLUMN_END = "end";
	public static final String JOB_TABLE_COLUMN_ERROR_DESC = "error_desc";

    public void init(String dbUrl, String dbPort, String dbName, String userName, String passwd,  int poolSize) throws Exception;
    public void createJob(String pid, String select, Config sourceConfig, Config targetConfig);
    public String getJobId(String pid);
    public void createMoveObjectTable(String jobId);
    public void createTargetObjectTable(String jobId);
    public void updateJobState(String jobId, IMOptions.WORK_TYPE type);
    public void insertErrorJob(String jobId, String msg);
    public boolean insertMoveObject(String jobId, boolean isFile, String mTime, long size, String path, String etag, String tag);
    public boolean insertMoveObjectVersioning(String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest);
    public boolean insertTargetObject(String jobId, String path, String versionId, long size, String etag);
    public boolean updateJobInfo(String jobId, long size);
    public boolean updateJobRerunInfo(String jobId, long size);
    public boolean insertRerunMoveObject(String jobId, boolean isFile, String mTime, long size, String path, String etag, String multipartInfo, String tag);
    public boolean insertRerunMoveObjectVersion(String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest);
    public Map<String, String> infoExistObjectVersion(String jobId, String path, String versionId);
    public Map<String, String> infoExistObject(String jobId, String path);
    public boolean updateSkipObject(String jobId, String path, String versionId);
    public boolean updateRerunSkipObject(String jobId, String path);
    public boolean updateRerunSkipObjectVersion(String jobId, String path, String versionId, boolean isLatest);
    public boolean updateJobRerunSkipInfo(String jobId, long size);
    public boolean updateJobSkipInfo(String jobId, long size);
    public boolean updateToMoveObject(String jobId, String mTime, long size, String path);
    public boolean updateObjectMove(String jobId, String path, String versionId);
    public boolean updateToMoveObjectVersion(String jobId, String mTime, long size, String path, String versionId);
    public boolean updateJobMoved(String jobId, long size);
    public boolean updateObjectMoveComplete(String jobId, String path, String versionId);
    public boolean updateObjectMoveEventFailed(String jobId, String path, String versionId, String errorCode, String errorMessage);
    public boolean updateJobFailedInfo(String jobId, long size);
    public void deleteCheckObjects(String jobId);
    public long getMaxSequence(String jobId);
    public List<HashMap<String, Object>> getToMoveObjectsInfo(String jobId, long sequence, long limit);
    public List<HashMap<String, Object>> getToDeleteObjectsInfo(String jobId, long sequence, long limit);
    public boolean updateJobDeleted(String jobId, long size);
    public void deleteObjects(String jobId, String path, String versionId);
    public void updateJobEnd(String jobId);
    public List<HashMap<String, Object>> status();
    public String getProcessId(String jobId);
    public void dropMoveObjectIndex(String jobId);
    public void dropMoveObjectTable(String jobId);
    public String getJobType(String jobId);
    public void updateJobRerun(String jobId);
    public void updateObjectsRerun(String jobId);
    public void updateJobStart(String jobId);
    public void setProcessId(String jobId, String pid);
    public int stateWhenExistObject(String jobId, String path);
    public String getMtime(String jobId, String path);
    public void updateDeleteMarker(String jobId, String path, String versionId);
    public boolean compareObject(String jobId, String path, long size, String etag);
    public boolean compareObject(String jobId, String path, String etag);
    public boolean compareObject(String jobId, String path, long size);
    public boolean isExistObject(String jobId, String path);
}
