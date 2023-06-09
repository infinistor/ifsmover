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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import ifs_mover.Config;
import ifs_mover.IMOptions;
import ifs_mover.MoveData;
import ifs_mover.Utils;

public class MariaDB implements MoverDB {
    protected Logger logger;
	private static HikariConfig config = new HikariConfig();
	private static HikariDataSource ds;

	private static final int JOB_ERROR = 10;

	private static final String IFS_FILE = "file";
	private static final String UNDER_OBJECTS = "_OBJECTS";
	private static final String UNDER_RERUN_OBJECTS = "_RERUN_OBJECTS";
	
	private static final String WHERE_JOB_ID = " WHERE job_id = ";

	
	private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS ";
	private static final String CREATE_JOB_TABLE =
			"CREATE TABLE IF NOT EXISTS `JOB` (\n"
			+ "`job_id` INT NOT NULL AUTO_INCREMENT,\n"
			+ "`job_state` INT NOT NULL DEFAULT '0',\n"
			+ "`pid` INT NOT NULL,\n"
			+ "`job_type` CHAR(5) NOT NULL,\n"
			+ "`source_point` VARCHAR(1024) NOT NULL,\n"
			+ "`target_point` VARCHAR(1024) NOT NULL, \n"
			+ "`objects_count` BIGINT DEFAULT '0',\n"
			+ "`objects_size` BIGINT DEFAULT '0',\n"
			+ "`moved_objects_count` BIGINT DEFAULT '0',\n"
			+ "`moved_objects_size` BIGINT DEFAULT '0',\n"
			+ "`failed_count` BIGINT DEFAULT '0',\n"
			+ "`failed_size` BIGINT DEFAULT '0',\n"
			+ "`skip_objects_count` BIGINT DEFAULT '0',\n"
			+ "`skip_objects_size` BIGINT DEFAULT '0',\n"
			+ "`delete_objects_count` BIGINT DEFAULT '0',\n"
			+ "`delete_objects_size` BIGINT DEFAULT '0',\n"
			+ "`start` VARCHAR(128),\n"
			+ "`end` VARCHAR(128),\n"
			+ "`error_desc` VARCHAR(512),\n"
			+ "PRIMARY KEY(`job_id`))ENGINE=InnoDB DEFAULT CHARSET=utf8;";
	
	private static final String UPDATE_JOB_ID = "UPDATE JOB_";
	private static final String INSERT_JOB_ID = "INSERT INTO JOB_";
	private static final String SQL_UPDATE_JOB_STATE_MOVE = "UPDATE JOB SET job_state = 1 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_COMPLETE = "UPDATE JOB SET job_state = 4 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_STOP = "UPDATE JOB SET job_state = 5 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_REMOVE = "UPDATE JOB SET job_state = 6 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_RERUN = "UPDATE JOB SET job_state = 7 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_RERUN_MOVE = "UPDATE JOB SET job_state = 8 WHERE job_id = ?";
	private static final String SQL_SELECT_JOB_STATUS = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB ORDER BY job_id";
	private static final String SQL_SELECT_JOB_STATUS_WITH_JOBID = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB WHERE job_id = ?";
	private static final String SQL_SELECT_JOB_STATUS_WITH_SRC_BUCKET = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB WHERE source_point LIKE ? ORDER BY job_id";
	private static final String SQL_SELECT_JOB_STATUS_WITH_DST_BUCKET = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB WHERE target_point LIKE ? ORDER BY job_id";
	private static final String SQL_SELECT_JOB_STATUS_WITH_SRC_DST_BUCKET = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB WHERE source_point LIKE ? AND target_point LIKE ? ORDER BY job_id";

	private static final String SQL_UPDATE_JOB_OBJECTS = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_COUNT_OBJECTS = "UPDATE JOB SET objects_count = objects_count + ?, objects_size = objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_FAILED_OBJECTS = "UPDATE JOB SET failed_count = failed_count + 1, failed_size = failed_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_FAILED_BATCH = "UPDATE JOB SET failed_count = failed_count + ?, failed_size = failed_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_RERUN_SKIP = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ?, skip_objects_count = skip_objects_count + 1, skip_objects_size = skip_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_RERUN_SKIPINFO = "UPDATE JOB SET skip_objects_count = skip_objects_count + ?, skip_objects_size = skip_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_SKIP = "UPDATE JOB SET skip_objects_count = skip_objects_count + 1, skip_objects_size = skip_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_RERUN_OBJECTS = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_RERUN_COUNT_OBJECTS = "UPDATE JOB SET objects_count = objects_count + ?, objects_size = objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_MOVED = "UPDATE JOB SET moved_objects_count = moved_objects_count + 1, moved_objects_size = moved_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_MOVED_BATCH = "UPDATE JOB SET moved_objects_count = moved_objects_count + ?, moved_objects_size = moved_objects_size + ? WHERE job_id = ?";
	// private static final String SQL_UPDATE_JOB_RERUN = "UPDATE JOB SET moved_objects_count = moved_objects_count + 1, moved_objects_size = moved_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_DELETED = "UPDATE JOB SET delete_objects_count = delete_objects_count + 1, delete_objects_size = delete_objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_ERROR = "UPDATE JOB SET job_state = ?, error_desc = ? WHERE job_id = ?";
	private static final String SQL_INSERT_JOB = "INSERT INTO JOB(pid, job_type, source_point, target_point, start) VALUES(?, ?, ?, ?, now())";
	private static final String SQL_UPDATE_JOB_START = "UPDATE JOB SET start = now() WHERE job_id =";
	private static final String SQL_UPDATE_JOB_END = "UPDATE JOB SET end = now() WHERE job_id =";
	private static final String SQL_INIT_JOB_RERUN = "UPDATE JOB SET objects_count = 0, objects_size = 0, moved_objects_count = 0, moved_objects_size = 0, failed_count = 0, failed_size = 0, skip_objects_count = 0, skip_objects_size = 0, delete_objects_count = 0, delete_objects_size = 0 WHERE job_id = ";
	private static final String SQL_INIT_MOVE_OBJECT_RERUN = "_OBJECTS SET skip_check = 0";
	private static final String SQL_INSERT_MOVE_OBJECT = "_OBJECTS (path, object_state, isfile, mtime, size, etag, multipart_info) VALUES(?, 1, ?, ?, ?, ?, NULL)";
	private static final String SQL_INSERT_RERUN_OBJECT = "_RERUN_OBJECTS (path, object_state, isfile, mtime, size, etag, multipart_info) VALUES(?, 1, ?, ?, ?, ?, NULL)";
	private static final String SQL_REPLACE = "REPLACE INTO JOB_";
	private static final String SQL_INSERT_TARGET_OBJECT = "_TARGET_OBJECTS (path, version_id, size, etag) VALUES(?, ?, ?, ?)";
	private static final String SQL_RERUN_INSERT_MOVE_OBJECT = "_OBJECTS (path, object_state, skip_check, isfile, mtime, size, etag, multipart_info, tag) VALUES(?, 1, 1, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_INSERT_MOVE_OBJECT_VERSIONING = "_OBJECTS (path, object_state, isfile, mtime, size, version_id, etag, multipart_info, isdelete, islatest) VALUES(?, 1, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_INSERT_RERUN_OBJECT_VERSIONING = "_RERUN_OBJECTS (path, object_state, isfile, mtime, size, version_id, etag, multipart_info, isdelete, islatest) VALUES(?, 1, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_RERUN_INSERT_MOVE_OBJECT_VERSIONING = "_OBJECTS (path, object_state, skip_check, isfile, mtime, size, version_id, etag, multipart_info, tag, isdelete, islatest) VALUES(?, 1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	// private static final String SQL_GET_MOVE_OBJECT_INFO = "SELECT path, isfile, size, object_state, mtime, version_id, etag, multipart_info, tag, isdelete, islatest, skip_check FROM JOB_";
	private static final String SQL_GET_MOVE_OBJECT_INFO = "SELECT path, isfile, size, object_state, mtime, version_id, etag, multipart_info, isdelete, islatest, skip_check FROM JOB_";
	private static final String SQL_GET_MOVE_OBJECT_INFO_VERSION = "SELECT path, isfile, size, object_state, mtime, version_id, etag, multipart_info, isdelete, islatest, skip_check FROM JOB_";
	private static final String SQL_GET_MOVE_OBJECT_INFO_WHERE = "_OBJECTS WHERE isdelete = 0 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 0 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 0 and sequence > ";
	private static final String SQL_GET_MOVE_OBJECT_INFO_WHERE_DELETE = "_OBJECTS WHERE isdelete = 1 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 1 and sequence > ";
	private static final String SQL_GET_RERUN_OBJECT_INFO_WHERE = "_RERUN_OBJECTS WHERE skip_check = 0 and isdelete = 0 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 0 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 0 and sequence > ";
	private static final String SQL_GET_RERUN_OBJECT_INFO_WHERE_DELETE = "_RERUN_OBJECTS WHERE skip_check = 0 and isdelete = 1 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 1 and sequence > ";
	private static final String SQL_ORDER_BY_PATH = " ORDER BY path LIMIT ";
	private static final String SQL_GET_OBJECT_STATE = "SELECT object_state FROM JOB_";
	private static final String SQL_GET_OBJECT_INFO = "SELECT object_state, mtime, etag FROM JOB_";
	private static final String SQL_GET_JOB_ID = "SELECT job_id FROM JOB WHERE pid = ";
	private static final String SQL_GET_JOB_TYPE = "SELECT job_type FROM JOB WHERE job_id = ";
	private static final String SQL_GET_MTIME = "SELECT mtime FROM JOB_";
	private static final String SQL_SET_PID = "UPDATE JOB SET pid = ";
	private static final String SQL_GET_PID = "SELECT pid FROM JOB WHERE job_id = ";
	private static final String SQL_GET_MAX_SEQUENCE = "SELECT MAX(sequence) FROM JOB_";
	private static final String SQL_DELETE_JOB = "DELETE FROM JOB_";
	private static final String SQL_GET_JOB_STATE = "SELECT job_state FROM JOB WHERE job_id = ";

	private static final String SQL_DROP_MOVE_OBJECT = "DROP TABLE JOB_";
	private static final String SQL_DROP_MOVE_OBJECT_INDEX = "DROP INDEX IF EXISTS idx_path ON JOB_";
	private static final String SQL_MTIME_INDEX = ",idx_mtime ON JOB_";
	// private static final String SQL_VERSIONID_INDEX = ",idx_versionid ON JOB_";
	private static final String SQL_OBJECT_STATE_INDEX = ",idx_state ON JOB_";

	private static final String SQL_RENAME_TABLE_SOURCE = "ALTER TABLE JOB_";
	private static final String SQL_RENAME_TABLE_TARGET = " RENAME TO JOB_";

	private static final String SQL_SKIP_CHECK = "_OBJECTS SET object_state = 1, skip_check = 1, mtime = '";
	private static final String SQL_SIZE = "', size = ";

	private static final String SQL_OBJECT_WHERE_PATH_ONLY = "_OBJECTS WHERE path = ?";
	private static final String SQL_SKIP_CHECK_WHERE_PATH = "_OBJECTS SET skip_check = 1 WHERE path = ?";
	private static final String SQL_OBJECT_WHERE_PATH_WITH_VERSIONID = "_OBJECTS WHERE path = ? and version_id = ?";
	private static final String SQL_OBJECT_WHERE_PATH_WITH_VERSIONID_NULL = "_OBJECTS WHERE path = ? and version_id is null";
	private static final String SQL_WHERE_PATH = " WHERE path = ?";
	private static final String SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL = " WHERE path = ? and version_id is null";
	private static final String SQL_WHERE_PATH_WITH_VERSIONID = " WHERE path = ? and version_id = ?";

	private static final String SQL_GET_TARGET = "SELECT path FROM JOB_";
	private static final String SQL_GET_TARGET_OBJECT = "_TARGET_OBJECTS WHERE path = ?";
	private static final String SQL_GET_TARGET_OBJECT_ETAG = "_TARGET_OBJECTS WHERE path = ? and etag = ?";
	private static final String SQL_GET_TARGET_OBJECT_SIZE = "_TARGET_OBJECTS WHERE path = ? and size = ?";

	private static final String SQL_GET_SKIP_RERUN_START = "SELECT count(*), sum(size) from JOB_";
	private static final String SQL_GET_SKIP_RERUN_END = "_RERUN_OBJECTS WHERE skip_check=1";

    private MariaDB() {
        logger = LoggerFactory.getLogger(MariaDB.class);
	}

    public static MariaDB getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
        private static final MariaDB INSTANCE = new MariaDB();
    }

	@Override
    public void init(String dbUrl, String dbPort, String dbName, String userName, String passwd,  int poolSize) throws Exception {				
		String jdbcUrl = "jdbc:mariadb://" + dbUrl + ":" + dbPort + "/" + dbName + "?useSSL=false&autoReconnect=true&createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf8";

		config.setJdbcUrl(jdbcUrl);
		config.setUsername(userName);
		config.setPassword(passwd);
		config.setDriverClassName("org.mariadb.jdbc.Driver");
		config.setConnectionTestQuery("select 1");
		config.addDataSourceProperty("maxPoolSize" , poolSize);
		config.addDataSourceProperty("minPoolSize" , 1);
		config.setPoolName("ifsmover");
		config.setMaximumPoolSize(poolSize);
		config.setMinimumIdle(1);

		ds = new HikariDataSource(config);
		
		createDB(dbName, userName, passwd);

		createTable();
    }

    public List<HashMap<String, Object>> select(String query, List<Object> params) throws Exception {
        List<HashMap<String, Object>> rmap = null; 
        
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rset = null;
        try {
			conn = ds.getConnection();
			pstmt = conn.prepareStatement(query);

            int index = 1;
			if(params != null) {
            	for(Object p : params) {
                	pstmt.setObject(index, p);
                	index++;
            	}
			}

			rset = pstmt.executeQuery();

            ResultSetMetaData md = rset.getMetaData();
            int columns = md.getColumnCount();
            int init = 0;
            while (rset.next()) {
                if(init == 0) {
                    rmap = new ArrayList<HashMap<String, Object>>();
                    init++;
                }

                HashMap<String, Object> map = null; 
                map = new HashMap<String, Object>(columns);
                for(int i=1; i<=columns; ++i) {
					map.put(md.getColumnName(i), rset.getObject(i));
				}
                rmap.add(map);
            }
		} catch (SQLException e) {
			logger.error(e.getMessage());
			throw new Exception("Error");
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception("Error");
		} finally {
			if ( rset != null ) try { rset.close(); } catch (Exception e) {logger.error(e.getMessage()); throw new Exception("Error");}
			if ( pstmt != null ) try { pstmt.close(); } catch (Exception e) {logger.error(e.getMessage()); throw new Exception("Error");}
			if ( conn != null ) try { conn.close(); } catch (Exception e) {logger.error(e.getMessage()); throw new Exception("Error");}
		}

        return rmap;
    }

	private void execute(String query, List<Object> params) throws Exception {
        try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {

            int index = 1;
			if(params != null) {
            	for(Object p : params) {
                	pstmt.setObject(index, p);
                	index++;
            	}
			}

			pstmt.execute();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			throw new Exception("Error");
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception("Error");
		}
    }

	private void createDB(String dbname, String userName, String userPasswd) throws Exception {
		String query = CREATE_DATABASE + dbname + ";";
		execute(query, null);
    }

	private void createTable() throws Exception {
		String query = CREATE_JOB_TABLE;
		execute(query, null);
	}

	@Override
	public void createJob(String pid, String select, Config sourceConfig, Config targetConfig) {
		List<Object> params = new ArrayList<Object>();
		params.add(pid);
		params.add(select);
		if (IFS_FILE.compareToIgnoreCase(select) == 0) {
			params.add(sourceConfig.getMountPoint() + sourceConfig.getPrefix());
		} else {
			params.add(sourceConfig.getBucket());
		}
		params.add(targetConfig.getBucket());

		try {
			execute(SQL_INSERT_JOB, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public String getJobId(String pid) {
		List<HashMap<String, Object>> resultList = null;
		String jobId = "";
		final String sql = SQL_GET_JOB_ID + pid;

		try {
			resultList = select(sql, null);
			if (resultList != null) {
				jobId = String.valueOf((int)resultList.get(0).get("job_id"));
				logger.info("jobId = {}", jobId);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return jobId;
	}

	@Override
	public void createMoveObjectTable(String jobId) {
		String query = "CREATE TABLE IF NOT EXISTS JOB_" + jobId + "_OBJECTS ("
				+ "`sequence` BIGINT AUTO_INCREMENT,"
				+ "`path` VARBINARY(2048) NOT NULL,"
				+ "`size` BIGINT NOT NULL,"
				+ "`object_state` INT NOT NULL DEFAULT 0,"
				+ "`isfile`	BOOLEAN default true,"
				+ "`skip_check` BOOLEAN DEFAULT false,"
				+ "`mtime` VARCHAR(26) NOT NULL,"
				+ "`version_id` VARCHAR(64),"
				+ "`etag` VARCHAR(64),"
				+ "`multipart_info` TEXT DEFAULT NULL,"
				// + "`tag` TEXT DEFAULT NULL,"
				+ "`isdelete` BOOLEAN DEFAULT false,"
				+ "`islatest` BOOLEAN DEFAULT false,"
				+ "`error_date` TEXT,"
				+ "`error_code` TEXT,"
				+ "`error_desc` TEXT,"
				+ "PRIMARY KEY(`sequence`), INDEX idx_path(`path`), INDEX idx_mtime(`mtime`), INDEX idx_state(`object_state`), INDEX idx_delete(`isdelete`))ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		try {
			execute(query, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public void createMoveObjectTableVersioning(String jobId) {
		String query = "CREATE TABLE IF NOT EXISTS JOB_" + jobId + "_OBJECTS ("
				+ "`sequence` BIGINT AUTO_INCREMENT,"
				+ "`path` VARBINARY(2048) NOT NULL,"
				+ "`size` BIGINT NOT NULL,"
				+ "`object_state` INT NOT NULL DEFAULT 0,"
				+ "`isfile`	BOOLEAN default true,"
				+ "`skip_check` BOOLEAN DEFAULT false,"
				+ "`mtime` VARCHAR(26) NOT NULL,"
				+ "`version_id` VARCHAR(64),"
				+ "`etag` VARCHAR(64),"
				+ "`multipart_info` TEXT DEFAULT NULL,"
				// + "`tag` TEXT DEFAULT NULL,"
				+ "`isdelete` BOOLEAN DEFAULT false,"
				+ "`islatest` BOOLEAN DEFAULT false,"
				+ "`error_date` TEXT,"
				+ "`error_code` TEXT,"
				+ "`error_desc` TEXT,"
				+ "PRIMARY KEY(`sequence`), INDEX idx_path(`path`), INDEX idx_mtime(`mtime`), INDEX idx_versionid(`version_id`), INDEX idx_state(`object_state`), INDEX idx_delete(`isdelete`), INDEX idx_latest(`islatest`))ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		try {
			execute(query, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void updateJobState(String jobId, IMOptions.WORK_TYPE type) {
		String sql = null;
		switch (type) {
		case MOVE:
			sql = SQL_UPDATE_JOB_STATE_MOVE;
			break;
			
		case COMPLETE:
			sql = SQL_UPDATE_JOB_STATE_COMPLETE;
			break;
			
		case STOP:
			sql = SQL_UPDATE_JOB_STATE_STOP;
			break;
			
		case REMOVE:
			sql = SQL_UPDATE_JOB_STATE_REMOVE;
			break;
			
		case RERUN:
			sql = SQL_UPDATE_JOB_STATE_RERUN;
			break;
			
		case RERUN_MOVE:
			sql = SQL_UPDATE_JOB_STATE_RERUN_MOVE;
			break;
			
		default:
			return;
		}

		List<Object> params = new ArrayList<Object>();
		params.add(jobId);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void insertErrorJob(String jobId, String msg) {
		String sql = SQL_UPDATE_JOB_ERROR;
		List<Object> params = new ArrayList<Object>();
		params.add(JOB_ERROR);
		params.add(msg);
		params.add(jobId);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public boolean insertMoveObject(String jobId, boolean isFile, String mTime, long size, String path, String etag, String tag) {
		String sql = INSERT_JOB_ID + jobId + SQL_INSERT_MOVE_OBJECT;
		List<Object> params = new ArrayList<Object>();
		params.add(path);
		params.add(isFile);
		params.add(mTime);
		params.add(size);
		if (etag == null || etag.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(etag);
		}
		// if (tag == null || tag.isEmpty()) {
		// 	params.add(java.sql.Types.NULL);
		// } else {
		// 	params.add(tag);
		// }

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean insertMoveObjectVersioning(String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest) {
		String sql = INSERT_JOB_ID + jobId + SQL_INSERT_MOVE_OBJECT_VERSIONING;
		List<Object> params = new ArrayList<Object>();
		params.add(path);
		params.add(isFile);
		params.add(mTime);
		params.add(size);
		if (versionId == null || versionId.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(versionId);
		}
		if (etag == null || etag.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(etag);
		}
		if (multipartInfo == null || multipartInfo.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(multipartInfo);
		}
		// if (tag == null || tag.isEmpty()) {
		// 	params.add(java.sql.Types.NULL);
		// } else {
		// 	params.add(tag);
		// }
		params.add(isDelete);
		params.add(isLatest);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateJobInfo(String jobId, long size) {
		String sql = SQL_UPDATE_JOB_OBJECTS;
		List<Object> params = new ArrayList<Object>();
		params.add(size);
		params.add(jobId);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateJobRerunInfo(String jobId, long size) {
		String sql = SQL_UPDATE_JOB_RERUN_OBJECTS;
		List<Object> params = new ArrayList<Object>();
		params.add(size);
		params.add(jobId);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean insertRerunMoveObject(String jobId, boolean isFile, String mTime, long size, String path, String etag, String multipartInfo, String tag) {
		String sql = INSERT_JOB_ID + jobId + SQL_RERUN_INSERT_MOVE_OBJECT;
		List<Object> params = new ArrayList<Object>();
		params.add(path);
		params.add(isFile);
		params.add(mTime);
		params.add(size);
		if (etag == null || etag.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(etag);
		}
		if (multipartInfo == null || etag.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(multipartInfo);
		}
		if (tag == null || etag.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(tag);
		}

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean insertRerunMoveObjectVersion(String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest) {
		String sql = INSERT_JOB_ID + jobId + SQL_RERUN_INSERT_MOVE_OBJECT_VERSIONING;
		List<Object> params = new ArrayList<Object>();
		params.add(path);
		params.add(isFile);
		params.add(mTime);
		params.add(size);
		if (versionId == null || versionId.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(versionId);
		}
		if (etag == null || etag.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(etag);
		}
		if (multipartInfo == null || multipartInfo.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(multipartInfo);
		}
		if (tag == null || tag.isEmpty()) {
			params.add(java.sql.Types.NULL);
		} else {
			params.add(tag);
		}
		params.add(isDelete);
		params.add(isLatest);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public Map<String, String> infoExistObjectVersion(String jobId, String path, String versionId) {
		Map<String, String> info = new HashMap<String, String>();	
		String sql;
		List<Object> params = new ArrayList<Object>();
		if (versionId == null || versionId.isEmpty()) {
			sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH_WITH_VERSIONID_NULL;
			params.add(path);
		} else {
			sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		}
		// if (versionId == null || versionId.isEmpty()) {
		// 	sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SQL_VERSIONID_IS_NULL;
		// } else {
		// 	sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		// }

		List<HashMap<String, Object>> resultList = null;

		try {
			resultList = select(sql, params);
			if (resultList != null) {
				info.put(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE, String.valueOf((int) resultList.get(0).get(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE)));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_MTIME, (String) resultList.get(0).get(MOVE_OBJECTS_TABLE_COLUMN_MTIME));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_ETAG, (String) resultList.get(0).get(MOVE_OBJECTS_TABLE_COLUMN_ETAG));
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return info;
	}

	@Override
	public Map<String, String> infoExistObject(String jobId, String path) {
		Map<String, String> info = new HashMap<String, String>();	
		List<Object> params = new ArrayList<Object>();
		String sql;
		// sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SINGLE_QUOTATION;
		sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH_ONLY;
		params.add(path);

		List<HashMap<String, Object>> resultList = null;

		try {
			resultList = select(sql, params);
			if (resultList != null) {
				info.put(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE, String.valueOf((int)resultList.get(0).get(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE)));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_MTIME, (String)resultList.get(0).get(MOVE_OBJECTS_TABLE_COLUMN_MTIME));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_ETAG, (String) resultList.get(0).get(MOVE_OBJECTS_TABLE_COLUMN_ETAG));
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return info;
	}

	@Override
	public boolean updateRerunSkipObject(String jobId, String path) {
		List<Object> params = new ArrayList<Object>();
		String sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK_WHERE_PATH;
		params.add(path);
		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateRerunSkipObjectVersion(String jobId, String path, String versionId, boolean isLatest) {
		List<Object> params = new ArrayList<Object>();
		String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET skip_check = 1, islatest = ";
		if (isLatest) {
			sql += "1";
		} else {
			sql += "0";
		}

		if (versionId == null || versionId.isEmpty()) {
			sql += SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
			// sql += " WHERE path = '" + path + SQL_VERSIONID_IS_NULL;
		} else {
			sql += SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
			// sql += " WHERE path = '" + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		}

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateJobRerunSkipInfo(String jobId, long size) {
		List<Object> params = new ArrayList<Object>();
		params.add(size);
		params.add(size);
		params.add(jobId);

		try {
			execute(SQL_UPDATE_JOB_RERUN_SKIP, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateToMoveObject(String jobId, String mTime, long size, String path) {
		List<Object> params = new ArrayList<Object>();
		String sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH;
		params.add(path);
		// String sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SINGLE_QUOTATION;
		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateObjectMove(String jobId, String path, String versionId) {		
		List<Object> params = new ArrayList<Object>();
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 2" + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		} else {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 2" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		}
		// if (versionId != null && !versionId.isEmpty()) {
		// 	sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 2 WHERE path = '" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		// } else {
		// 	sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 2 WHERE path = '" + path + "' and version_id is null";
		// }

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateToMoveObjectVersion(String jobId, String mTime, long size, String path, String versionId) {
		List<Object> params = new ArrayList<Object>();
		String sql;
		if (versionId == null || versionId.isEmpty()) { 
			sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		} else {
			sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		}
		// if (versionId == null || versionId.isEmpty()) { 
		// 	sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SQL_VERSIONID_IS_NULL;
		// } else {
		// 	sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		// }

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateJobMoved(String jobId, long size) {
		List<Object> params = new ArrayList<Object>();
		params.add(size);
		params.add(jobId);

		try {
			execute(SQL_UPDATE_JOB_MOVED, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateObjectMoveComplete(String jobId, String path, String versionId) {
		List<Object> params = new ArrayList<Object>();
		String sql = UPDATE_JOB_ID + jobId;
		if (versionId != null && !versionId.isEmpty()) {
			sql += "_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		} else {
			sql += "_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		}
		// if (versionId != null && !versionId.isEmpty()) {
		// 	sql += "_OBJECTS SET object_state = 3 WHERE path ='" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		// } else {
		// 	sql += "_OBJECTS SET object_state = 3 WHERE path ='" + path + "' and version_id is null";
		// }

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateObjectMoveEventFailed(String jobId, String path, String versionId, String errorCode, String errorMessage) {
		List<Object> params = new ArrayList<Object>();
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 4, error_date = now(), error_code ='" + errorCode + "', error_desc = '" + errorMessage + "'" + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		} else {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 4, error_date = now(), error_code ='" + errorCode + "', error_desc = '" + errorMessage + "'" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		}

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateJobFailedInfo(String jobId, long size) {
		List<Object> params = new ArrayList<Object>();
		params.add(size);
		params.add(jobId);

		try {
			execute(SQL_UPDATE_JOB_FAILED_OBJECTS, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public void deleteCheckObjects(String jobId) {
		String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 1 WHERE skip_check = 0";
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public long getMaxSequence(String jobId) {
		List<HashMap<String, Object>> resultList = null;
		long maxSequence = 0;
		String sql = SQL_GET_MAX_SEQUENCE + jobId + UNDER_OBJECTS;

		try {
			resultList = select(sql, null);
			if (resultList != null) {
				if (resultList.get(0).get("MAX(sequence)") != null) {
					maxSequence = (long)resultList.get(0).get("MAX(sequence)");
				}
			}
		} catch (Exception e) {
			Utils.logging(logger, e);
		}

		return maxSequence;
	}

	@Override
	public List<MoveData> getToMoveObjectsInfo(String jobId, long sequence, long limit) {
		List<MoveData> resultList = new ArrayList<MoveData>();

		// String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
		// String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + "_OBJECTS WHERE sequence > " + sequence + " AND isdelete = 0 LIMIT " + limit;
		// String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + "_OBJECTS WHERE sequence > " + sequence + " LIMIT " + limit;
		String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + "_OBJECTS WHERE sequence > " + sequence + " and sequence <= " + (sequence + limit);
		try (Connection conn = ds.getConnection();
				PreparedStatement pstmt = conn.prepareStatement(sql);
				ResultSet rset = pstmt.executeQuery()) {
			ResultSetMetaData md = rset.getMetaData();
            int columns = md.getColumnCount();
            while (rset.next()) {
				MoveData data = new MoveData();
                for(int i=1; i<=columns; ++i) {
					if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISDELETE)) {
						data.setDelete(rset.getBoolean(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISLATEST)) {
						data.setLatest(rset.getBoolean(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH)) {
						data.setPath(rset.getString(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID)) {
						data.setVersionId(rset.getString(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISFILE)) {
						data.setFile(rset.getBoolean(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_SIZE)) {
						data.setSize(rset.getLong(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_SKIP_CHECK)) {
						data.setSkipCheck(rset.getBoolean(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ETAG)) {
						data.setETag(rset.getString(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MULTIPART_INFO)) {
						data.setMultiPartInfo(rset.getString(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE)) {
						data.setObjectState(rset.getInt(i));
					}
				}
				resultList.add(data);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return resultList;
	}

	public List<HashMap<String, Object>> getToMoveObjectsInfoVersioning(String jobId, long sequence, long limit) {
		List<HashMap<String, Object>> resultList = null;

		// String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
		String sql = SQL_GET_MOVE_OBJECT_INFO_VERSION + jobId + "_OBJECTS WHERE sequence > " + sequence + " AND isdelete = 0 AND islastet = 0 LIMIT " + limit;
		try {
			resultList = select(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return resultList;
	}

	public List<HashMap<String, Object>> getToMoveLatestObjectsInfoVersioning(String jobId, long sequence, long limit) {
		List<HashMap<String, Object>> resultList = null;

		// String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
		String sql = SQL_GET_MOVE_OBJECT_INFO_VERSION + jobId + "_OBJECTS WHERE sequence > " + sequence + " AND isdelete = 0 AND islatest = 1 LIMIT " + limit;
		try {
			resultList = select(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return resultList;
	}

	@Override
	public List<HashMap<String, Object>> getToDeleteObjectsInfo(String jobId, long sequence, long limit) {
		List<HashMap<String, Object>> resultList = null;

		// String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE_DELETE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
		String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + "_OBJECTS WHERE sequence > " + sequence + " AND isdelete = 1 LIMIT " + limit;
		try {
			resultList = select(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return resultList;
	}

	public List<HashMap<String, Object>> getToDeleteObjectsInfoVersioning(String jobId, long sequence, long limit) {
		List<HashMap<String, Object>> resultList = null;

		// String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE_DELETE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
		String sql = SQL_GET_MOVE_OBJECT_INFO_VERSION + jobId + "_OBJECTS WHERE sequence > " + sequence + " AND isdelete = 1 AND islatest = 1 LIMIT " + limit;
		try {
			resultList = select(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return resultList;
	}

	@Override
	public boolean updateJobDeleted(String jobId, long size) {
		List<Object> params = new ArrayList<Object>();
		params.add(size);
		params.add(jobId);

		try {
			execute(SQL_UPDATE_JOB_DELETED, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public void deleteObjects(String jobId, String path, String versionId) {
		List<Object> params = new ArrayList<Object>();
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = SQL_DELETE_JOB + jobId + "_OBJECTS" + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		} else {
			sql = SQL_DELETE_JOB + jobId + "_OBJECTS" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		}
		// if (versionId != null && !versionId.isEmpty()) {
		// 	sql = SQL_DELETE_JOB + jobId + "_OBJECTS WHERE path = '" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		// } else {
		// 	sql = SQL_DELETE_JOB + jobId + "_OBJECTS WHERE path = '" + path + "' and version_id is null";
		// }

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void updateJobEnd(String jobId) {
		try {
			execute(SQL_UPDATE_JOB_END + jobId, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public List<HashMap<String, Object>> status() {
		try {
			return select(SQL_SELECT_JOB_STATUS, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return null;
	}

	@Override
	public List<HashMap<String, Object>> status(String jobId) {
		try {
			List<Object> params = new ArrayList<Object>();
			params.add(jobId);
			return select(SQL_SELECT_JOB_STATUS_WITH_JOBID, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return null;
	}

	@Override
	public List<HashMap<String, Object>> status(String srcBucketName, String dstBucketName) {
		try {
			List<Object> params = new ArrayList<Object>();
			params.add("%" + srcBucketName + "%");
			params.add("%" + dstBucketName + "%");
			return select(SQL_SELECT_JOB_STATUS_WITH_SRC_DST_BUCKET, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return null;
	}

	@Override
	public List<HashMap<String, Object>> statusSrcBucket(String bucket) {
		try {
			List<Object> params = new ArrayList<Object>();
			params.add("%" + bucket + "%");
			return select(SQL_SELECT_JOB_STATUS_WITH_SRC_BUCKET, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return null;
	}

	@Override
	public List<HashMap<String, Object>> statusDstBucket(String bucket) {
		try {
			List<Object> params = new ArrayList<Object>();
			params.add("%" + bucket + "%");
			return select(SQL_SELECT_JOB_STATUS_WITH_DST_BUCKET, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return null;
	}

	@Override
	public String getProcessId(String jobId) {
		List<HashMap<String, Object>> resultList = null;
		String pid = "";
		final String sql = SQL_GET_PID + jobId;

		try {
			resultList = select(sql, null);
			if (resultList != null) {
				pid = String.valueOf((int)resultList.get(0).get("pid"));
				logger.info("pid = {}", pid);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return pid;
	}

	@Override
	public void dropMoveObjectIndex(String jobId) {
		// String sql = SQL_DROP_MOVE_OBJECT_INDEX + jobId + UNDER_OBJECTS + SQL_MTIME_INDEX + jobId + UNDER_OBJECTS + SQL_OBJECT_STATE_INDEX + jobId + UNDER_OBJECTS;
		String sql = "ALTER TABLE JOB_" + jobId + "_OBJECTS DROP INDEX ALL";
		logger.info("sql : {}", sql);
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void dropMoveObjectTable(String jobId) {
		String sql = SQL_DROP_MOVE_OBJECT + jobId + UNDER_OBJECTS;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public String getJobType(String jobId) {
		List<HashMap<String, Object>> resultList = null;
		String jobType = "";
		final String sql = SQL_GET_JOB_TYPE + jobId;

		try {
			resultList = select(sql, null);
			if (resultList != null) {
				jobType = (String) resultList.get(0).get("job_type");
				logger.info("job type = {}", jobType);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return jobType;
	}

	@Override
	public void updateJobRerun(String jobId) {
		String sql = SQL_INIT_JOB_RERUN + jobId;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	
	@Override
	public void updateObjectsRerun(String jobId) {
		String sql = UPDATE_JOB_ID + jobId + SQL_INIT_MOVE_OBJECT_RERUN;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void updateJobStart(String jobId) {
		String sql = SQL_UPDATE_JOB_START + jobId;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void setProcessId(String jobId, String pid) {
		String sql = SQL_SET_PID + pid + WHERE_JOB_ID + jobId;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public int stateWhenExistObject(String jobId, String path) {
		List<HashMap<String, Object>> resultList = null;
		int state = -1;
		List<Object> params = new ArrayList<Object>();
		String sql = SQL_GET_OBJECT_STATE + jobId + SQL_OBJECT_WHERE_PATH_ONLY;
		params.add(path);

		try {
			resultList = select(sql, params);
			if (resultList != null) {
				state = (int) resultList.get(0).get("object_state");
				logger.info("object_state = {}", state);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return state;
	}

	@Override
	public String getMtime(String jobId, String path) {
		List<HashMap<String, Object>> resultList = null;
		String mtime = null;
		List<Object> params = new ArrayList<Object>();
		String sql = SQL_GET_MTIME + jobId + SQL_OBJECT_WHERE_PATH_ONLY;
		params.add(path);
		// String sql = SQL_GET_MTIME + jobId + SQL_OBJECT_WHERE_PATH + path + SINGLE_QUOTATION;

		try {
			resultList = select(sql, params);
			if (resultList != null) {
				mtime = (String) resultList.get(0).get("mtime");
				logger.info("mtime = {}", mtime);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return mtime;
	}

	@Override
	public void updateDeleteMarker(String jobId, String path, String versionId) {
		List<Object> params = new ArrayList<Object>();
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		} else {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		}

		// String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 3 WHERE path ='" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void createTargetObjectTable(String jobId) {
		String query = "CREATE TABLE IF NOT EXISTS JOB_" + jobId + "_TARGET_OBJECTS (\n"
				+ "`path` VARBINARY(2048) NOT NULL,\n"
				+ "`version_id` VARCHAR(64),\n"
				+ "`size` BIGINT NOT NULL,\n"
				+ "`etag` VARCHAR(64),\n"
				+ "PRIMARY KEY(`path`, `version_id`))ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		try {
			execute(query, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public boolean compareObject(String jobId, String path, long size, String etag) {
		List<HashMap<String, Object>> resultList = null;
		List<Object> params = new ArrayList<Object>();
		String sql = null;
		
		if (etag.lastIndexOf("-") != -1) {
			sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_SIZE;
			params.add(path);
			params.add(size);
		} else {
			sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_ETAG;
			params.add(path);
			params.add(etag);
		}

		try {
			resultList = select(sql, params);
			if (resultList != null) {
				return true;
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return false;
	}

	@Override
	public boolean insertTargetObject(String jobId, String path, String versionId, long size, String etag) {
		String sql = SQL_REPLACE + jobId + SQL_INSERT_TARGET_OBJECT;
		List<Object> params = new ArrayList<Object>();
		params.add(path);
		params.add(versionId);
		params.add(size);
		params.add(etag);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateSkipObject(String jobId, String path, String versionId) {
		List<Object> params = new ArrayList<Object>();
		String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 3, skip_check = 1";
		
		if (versionId == null || versionId.isEmpty()) {
			sql += SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		} else {
			sql += SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		}
		// if (versionId == null || versionId.isEmpty()) {
		// 	sql += " WHERE path = '" + path + SQL_VERSIONID_IS_NULL;
		// } else {
		// 	sql += " WHERE path = '" + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		// }

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateJobSkipInfo(String jobId, long size) {
		List<Object> params = new ArrayList<Object>();
		params.add(size);
		params.add(jobId);

		try {
			execute(SQL_UPDATE_JOB_SKIP, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean compareObject(String jobId, String path, String etag) {
		List<HashMap<String, Object>> resultList = null;
		List<Object> params = new ArrayList<Object>();

		String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_ETAG;
		params.add(path);
		params.add(etag);

		try {
			resultList = select(sql, params);
			if (resultList != null) {
				return true;
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return false;
	}

	@Override
	public boolean compareObject(String jobId, String path, long size) {
		List<HashMap<String, Object>> resultList = null;
		List<Object> params = new ArrayList<Object>();

		String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_SIZE;
		params.add(path);
		params.add(size);

		try {
			resultList = select(sql, params);
			if (resultList != null) {
				return true;
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return false;
	}

	@Override
	public boolean isExistObject(String jobId, String path) {
		List<HashMap<String, Object>> resultList = null;
		List<Object> params = new ArrayList<Object>();
		
		String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT;
		params.add(path);

		try {
			resultList = select(sql, params);
			if (resultList != null) {
				return true;
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return false;
	}

	@Override
	public long insertMoveObject(String jobId, ObjectListing objectListing) {
		long totalSize = 0;
		String query = INSERT_JOB_ID + jobId + SQL_INSERT_MOVE_OBJECT;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				pstmt.setObject(1, objectSummary.getKey());
				pstmt.setObject(2, objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/');
				pstmt.setObject(3, String.valueOf(objectSummary.getLastModified().getTime()));
				pstmt.setObject(4, objectSummary.getSize());
				if (objectSummary.getETag() == null || objectSummary.getETag().isEmpty()) {
					pstmt.setObject(5, java.sql.Types.NULL);
				} else {
					pstmt.setObject(5, objectSummary.getETag());
				}
				// pstmt.setObject(6, java.sql.Types.NULL);
				pstmt.addBatch();
				totalSize += objectSummary.getSize();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return totalSize;
	}

	@Override
	public long insertMoveObjectVersioning(String jobId, VersionListing versionListing) {
		long totalSize = 0;
		String query = INSERT_JOB_ID + jobId + SQL_INSERT_MOVE_OBJECT_VERSIONING;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
				pstmt.setObject(1, versionSummary.getKey());
				pstmt.setObject(2, versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/');
				pstmt.setObject(3, String.valueOf(versionSummary.getLastModified().getTime()));
				pstmt.setObject(4, versionSummary.getSize());
				if (versionSummary.getVersionId() == null || versionSummary.getVersionId().isEmpty()) {
					pstmt.setObject(5, java.sql.Types.NULL);
				} else {
					pstmt.setObject(5, versionSummary.getVersionId());
				}
				if (versionSummary.getETag() == null || versionSummary.getETag().isEmpty()) {
					pstmt.setObject(6, java.sql.Types.NULL);
				} else {
					pstmt.setObject(6, versionSummary.getETag());
				}
				pstmt.setObject(7, java.sql.Types.NULL);
				// pstmt.setObject(8, java.sql.Types.NULL);
				pstmt.setObject(8, versionSummary.isDeleteMarker());
				pstmt.setObject(9, versionSummary.isLatest());
				pstmt.addBatch();
				totalSize += versionSummary.getSize();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return totalSize;
	}

	@Override
	public long insertRerunObject(String jobId, ObjectListing objectListing) {
		long totalSize = 0;
		String query = INSERT_JOB_ID + jobId + SQL_INSERT_RERUN_OBJECT;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				pstmt.setObject(1, objectSummary.getKey());
				pstmt.setObject(2, objectSummary.getKey().charAt(objectSummary.getKey().length() - 1) != '/');
				pstmt.setObject(3, String.valueOf(objectSummary.getLastModified().getTime()));
				pstmt.setObject(4, objectSummary.getSize());
				if (objectSummary.getETag() == null || objectSummary.getETag().isEmpty()) {
					pstmt.setObject(5, java.sql.Types.NULL);
				} else {
					pstmt.setObject(5, objectSummary.getETag());
				}
				// pstmt.setObject(6, java.sql.Types.NULL);
				pstmt.addBatch();
				totalSize += objectSummary.getSize();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return totalSize;
	}

	@Override
	public long insertRerunObjectVersioning(String jobId, VersionListing versionListing) {
		long totalSize = 0;
		String query = INSERT_JOB_ID + jobId + SQL_INSERT_RERUN_OBJECT_VERSIONING;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
				pstmt.setObject(1, versionSummary.getKey());
				pstmt.setObject(2, versionSummary.getKey().charAt(versionSummary.getKey().length() - 1) != '/');
				pstmt.setObject(3, String.valueOf(versionSummary.getLastModified().getTime()));
				pstmt.setObject(4, versionSummary.getSize());
				if (versionSummary.getVersionId() == null || versionSummary.getVersionId().isEmpty()) {
					pstmt.setObject(5, java.sql.Types.NULL);
				} else {
					pstmt.setObject(5, versionSummary.getVersionId());
				}
				if (versionSummary.getETag() == null || versionSummary.getETag().isEmpty()) {
					pstmt.setObject(6, java.sql.Types.NULL);
				} else {
					pstmt.setObject(6, versionSummary.getETag());
				}
				pstmt.setObject(7, java.sql.Types.NULL);
				pstmt.setObject(8, versionSummary.isDeleteMarker());
				pstmt.setObject(9, versionSummary.isLatest());
				pstmt.addBatch();
				totalSize += versionSummary.getSize();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return totalSize;
	}

	@Override
	public boolean updateJobInfo(String jobId, int count, long size) {
		String sql = SQL_UPDATE_JOB_COUNT_OBJECTS;
		List<Object> params = new ArrayList<Object>();
		params.add(count);
		params.add(size);
		params.add(jobId);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateJobRerunInfo(String jobId, int count, long size) {
		String sql = SQL_UPDATE_JOB_RERUN_COUNT_OBJECTS;
		List<Object> params = new ArrayList<Object>();
		params.add(count);
		params.add(size);
		params.add(jobId);

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public void dropRerunObjectIndex(String jobId) {
		// String sql = SQL_DROP_MOVE_OBJECT_INDEX + jobId + UNDER_RERUN_OBJECTS + SQL_MTIME_INDEX + jobId + UNDER_RERUN_OBJECTS + SQL_OBJECT_STATE_INDEX + jobId + UNDER_RERUN_OBJECTS;
		String sql = "ALTER TABLE JOB_" + jobId + "_RERUN_OBJECTS DROP INDEX ALL";
		logger.info("sql : {}", sql);
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void dropRerunObjectTable(String jobId) {
		String sql = SQL_DROP_MOVE_OBJECT + jobId + UNDER_RERUN_OBJECTS;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void createRerunObjectTable(String jobId) {
		String query = "CREATE TABLE IF NOT EXISTS JOB_" + jobId + "_RERUN_OBJECTS ("
				+ "`sequence` BIGINT AUTO_INCREMENT,"
				+ "`path` VARBINARY(2048) NOT NULL,"
				+ "`size` BIGINT NOT NULL,"
				+ "`object_state` INT NOT NULL DEFAULT 0,"
				+ "`isfile`	BOOLEAN default true,"
				+ "`skip_check` BOOLEAN DEFAULT false,"
				+ "`mtime` VARCHAR(26) NOT NULL,"
				+ "`version_id` VARCHAR(64),"
				+ "`etag` VARCHAR(64),"
				+ "`multipart_info` TEXT DEFAULT NULL,"
				// + "`tag` TEXT DEFAULT NULL,"
				+ "`isdelete` BOOLEAN DEFAULT false,"
				+ "`islatest` BOOLEAN DEFAULT false,"
				+ "`error_date` TEXT,"
				+ "`error_code` TEXT,"
				+ "`error_desc` TEXT,"
				+ "PRIMARY KEY(`sequence`), INDEX idx_path(`path`), INDEX idx_mtime(`mtime`), INDEX idx_state(`object_state`), INDEX idx_delete(`isdelete`))ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		try {
			execute(query, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public void createRerunObjectTableVersioning(String jobId) {
		String query = "CREATE TABLE IF NOT EXISTS JOB_" + jobId + "_RERUN_OBJECTS ("
				+ "`sequence` BIGINT AUTO_INCREMENT,"
				+ "`path` VARBINARY(2048) NOT NULL,"
				+ "`size` BIGINT NOT NULL,"
				+ "`object_state` INT NOT NULL DEFAULT 0,"
				+ "`isfile`	BOOLEAN default true,"
				+ "`skip_check` BOOLEAN DEFAULT false,"
				+ "`mtime` VARCHAR(26) NOT NULL,"
				+ "`version_id` VARCHAR(64),"
				+ "`etag` VARCHAR(64),"
				+ "`multipart_info` TEXT DEFAULT NULL,"
				// + "`tag` TEXT DEFAULT NULL,"
				+ "`isdelete` BOOLEAN DEFAULT false,"
				+ "`islatest` BOOLEAN DEFAULT false,"
				+ "`error_date` TEXT,"
				+ "`error_code` TEXT,"
				+ "`error_desc` TEXT,"
				+ "PRIMARY KEY(`sequence`), INDEX idx_path(`path`), INDEX idx_mtime(`mtime`), INDEX idx_versionid(`version_id`), INDEX idx_state(`object_state`), INDEX idx_delete(`isdelete`), INDEX idx_latest(`islatest`))ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		try {
			execute(query, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void renameTable(String jobId) {
		String sql = SQL_RENAME_TABLE_SOURCE + jobId + UNDER_RERUN_OBJECTS + SQL_RENAME_TABLE_TARGET + jobId + UNDER_OBJECTS;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public long getMaxSequenceRerun(String jobId) {
		List<HashMap<String, Object>> resultList = null;
		long maxSequence = 0;
		String sql = SQL_GET_MAX_SEQUENCE + jobId + UNDER_RERUN_OBJECTS;

		try {
			resultList = select(sql, null);
			if (resultList != null) {
				if (resultList.get(0).get("MAX(sequence)") != null) {
					maxSequence = (long)resultList.get(0).get("MAX(sequence)");
				}
			}
		} catch (Exception e) {
			Utils.logging(logger, e);
		}

		return maxSequence;
	}

	@Override
	public List<MoveData> getToRerunObjectsInfo(String jobId, long sequence, long limit) {
		List<MoveData> resultList = new ArrayList<MoveData>();
		String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + "_RERUN_OBJECTS WHERE sequence > " + sequence + " and sequence <= " + (sequence + limit);
		try (Connection conn = ds.getConnection();
				PreparedStatement pstmt = conn.prepareStatement(sql);
				ResultSet rset = pstmt.executeQuery()) {
			ResultSetMetaData md = rset.getMetaData();
            int columns = md.getColumnCount();
            while (rset.next()) {
				MoveData data = new MoveData();
                for(int i=1; i<=columns; ++i) {
					if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISDELETE)) {
						data.setDelete(rset.getBoolean(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISLATEST)) {
						data.setLatest(rset.getBoolean(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_PATH)) {
						data.setPath(rset.getString(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_VERSIONID)) {
						data.setVersionId(rset.getString(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ISFILE)) {
						data.setFile(rset.getBoolean(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_SIZE)) {
						data.setSize(rset.getLong(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_SKIP_CHECK)) {
						data.setSkipCheck(rset.getBoolean(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_ETAG)) {
						data.setETag(rset.getString(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_MULTIPART_INFO)) {
						data.setMultiPartInfo(rset.getString(i));
					} else if (md.getColumnName(i).equals(MoverDB.MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE)) {
						data.setObjectState(rset.getInt(i));
					}
				}
				resultList.add(data);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return resultList;
	}

	@Override
	public List<HashMap<String, Object>> getToRerunDeleteObjectsInfo(String jobId, long sequence, long limit) {
		List<HashMap<String, Object>> resultList = null;

		String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_RERUN_OBJECT_INFO_WHERE_DELETE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
		try {
			resultList = select(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return resultList;
	}

	@Override
	public void updateSkipRerun(String jobId) {
		String sql = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS A, (select * from JOB_" + jobId + "_OBJECTS where object_state=3) B set A.skip_check=1, A.object_state=3 where A.path=B.path and A.mtime=B.mtime;";
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public boolean updateJobRerunSkipInfo(String jobId, long count, long size) {
		List<Object> params = new ArrayList<Object>();
		params.add(count);
		params.add(size);
		params.add(jobId);

		try {
			execute(SQL_UPDATE_JOB_RERUN_SKIPINFO, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public void infoSkipRerun(String jobId) {
		String sql = SQL_GET_SKIP_RERUN_START + jobId + SQL_GET_SKIP_RERUN_END;

		List<HashMap<String, Object>> resultList = null;

		try {
			resultList = select(sql, null);
			if (resultList != null) {
				long totalCount = Long.parseLong(String.valueOf(resultList.get(0).get("count(*)")));
				if (totalCount > 0) {
					long totalSize = Long.parseLong(String.valueOf(resultList.get(0).get("sum(size)")));
					logger.info("total count : {}, total size : {}", totalCount, totalSize);
					updateJobRerunSkipInfo(jobId, totalCount, totalSize);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public boolean isExistMoveTable(String db, String jobId) {
		String sql;
		sql = "SELECT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + db + "' AND TABLE_NAME='JOB_" + jobId + "_OBJECTS') AS flag;";

		List<HashMap<String, Object>> resultList = null;

		try {
			resultList = select(sql, null);
			if (resultList != null) {
				int flag = (int)resultList.get(0).get("flag");
				if (flag == 1) {
					return true;
				} else {
					return false;
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return false;
	}

	@Override
	public boolean isExistRerunTable(String db, String jobId) {
		String sql;
		sql = "SELECT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + db + "' AND TABLE_NAME='JOB_" + jobId + "_RERUN_OBJECTS') AS flag;";

		List<HashMap<String, Object>> resultList = null;

		try {
			resultList = select(sql, null);
			if (resultList != null) {
				int flag = (int)resultList.get(0).get("flag");
				if (flag == 1) {
					return true;
				} else {
					return false;
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return false;
	}

	@Override
	public boolean updateObjectRerunComplete(String jobId, String path, String versionId) {
		List<Object> params = new ArrayList<Object>();
		String sql = UPDATE_JOB_ID + jobId;
		if (versionId != null && !versionId.isEmpty()) {
			sql += "_RERUN_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		} else {
			sql += "_RERUN_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		}

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateObjectRerunEventFailed(String jobId, String path, String versionId, String errorCode,
			String errorMessage) {
			List<Object> params = new ArrayList<Object>();
			String sql;
			if (versionId != null && !versionId.isEmpty()) {
				sql = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS SET object_state = 4, error_date = now(), error_code ='" + errorCode + "', error_desc = '" + errorMessage + "'" + SQL_WHERE_PATH_WITH_VERSIONID;
				params.add(path);
				params.add(versionId);
			} else {
				sql = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS SET object_state = 4, error_date = now(), error_code ='" + errorCode + "', error_desc = '" + errorMessage + "'" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
				params.add(path);
			}
	
			try {
				execute(sql, params);
			} catch (Exception e) {
				logger.error(e.getMessage());
				return false;
			}
	
			return true;
	}

	@Override
	public boolean updateObjectRerun(String jobId, String path, String versionId) {
		List<Object> params = new ArrayList<Object>();
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS SET object_state = 2" + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		} else {
			sql = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS SET object_state = 2" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		}
		// if (versionId != null && !versionId.isEmpty()) {
		// 	sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 2 WHERE path = '" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		// } else {
		// 	sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 2 WHERE path = '" + path + "' and version_id is null";
		// }

		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}
		return true;
	}

	@Override
	public void updateRerunDeleteMarker(String jobId, String path, String versionId) {
		List<Object> params = new ArrayList<Object>();
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID;
			params.add(path);
			params.add(versionId);
		} else {
			sql = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID_IS_NULL;
			params.add(path);
		}

		// String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 3 WHERE path ='" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		try {
			execute(sql, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void checkDeleteObjectsForReRun(String jobId) {
		String sql = "insert into JOB_" + jobId + "_RERUN_OBJECTS(path,size,object_state,mtime,etag,isdelete, islatest) select A.path, A.size, 1, A.mtime, A.etag, 1, 1 from JOB_" + jobId + "_OBJECTS A left outer join JOB_" + jobId + "_RERUN_OBJECTS B on A.path=B.path where B.path is null";
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public boolean updateJobMoved(String jobId, int count, long size) {
		List<Object> params = new ArrayList<Object>();
		params.add(count);
		params.add(size);
		params.add(jobId);

		try {
			execute(SQL_UPDATE_JOB_MOVED_BATCH, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateJobFailedInfo(String jobId, int count, long size) {
		List<Object> params = new ArrayList<Object>();
		params.add(count);
		params.add(size);
		params.add(jobId);

		try {
			execute(SQL_UPDATE_JOB_FAILED_BATCH, params);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateObjectMoveComplete(String jobId, List<HashMap<String, Object>> list) {
		String query = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (HashMap<String, Object> map : list) {
				pstmt.setObject(1, map.get("path"));
				if (map.get("versionId") == null || map.get("versionId").equals("")) {
					pstmt.setObject(2, java.sql.Types.NULL);
				} else {
					pstmt.setObject(2, map.get("versionId"));
				}
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			return true;
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return false;
	}

	@Override
	public boolean updateObjectRerunComplete(String jobId, List<HashMap<String, Object>> list) {
		String query = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS SET object_state = 3" + SQL_WHERE_PATH_WITH_VERSIONID;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (HashMap<String, Object> map : list) {
				pstmt.setObject(1, map.get("path"));
				if (map.get("versionId") == null || map.get("versionId").equals("")) {
					pstmt.setObject(2, java.sql.Types.NULL);
				} else {
					pstmt.setObject(2, map.get("versionId"));
				}
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			return true;
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return false;
	}

	@Override
	public boolean updateObjectMoveEventFailed(String jobId, List<HashMap<String, Object>> list) {
		String query = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 4, error_date = now()" + SQL_WHERE_PATH_WITH_VERSIONID;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (HashMap<String, Object> map : list) {
				pstmt.setObject(1, map.get("path"));
				if (map.get("versionId") == null || map.get("versionId").equals("")) {
					pstmt.setObject(2, java.sql.Types.NULL);
				} else {
					pstmt.setObject(2, map.get("versionId"));
				}
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			return true;
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return false;
	}

	@Override
	public boolean updateObjectRerunEventFailed(String jobId, List<HashMap<String, Object>> list) {
		String query = UPDATE_JOB_ID + jobId + "_RERUN_OBJECTS SET object_state = 4, error_date = now()" + SQL_WHERE_PATH_WITH_VERSIONID;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (HashMap<String, Object> map : list) {
				pstmt.setObject(1, map.get("path"));
				if (map.get("versionId") == null || map.get("versionId").equals("")) {
					pstmt.setObject(2, java.sql.Types.NULL);
				} else {
					pstmt.setObject(2, map.get("versionId"));
				}
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			return true;
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return false;
	}

	@Override
	public boolean updateJobResult(String jobId, boolean result, String path, String versionId, long size,
			boolean isRerun) {
		List<Object> params = new ArrayList<Object>();
		params.add(size);
		params.add(jobId);
		String query1 = null;
		String sql = null;
		if (result) {
			query1 = SQL_UPDATE_JOB_MOVED;
			sql = UPDATE_JOB_ID + jobId;
			if (isRerun) {
				if (versionId != null && !versionId.isEmpty()) {
					sql += "_RERUN_OBJECTS SET object_state = 3" + " WHERE path ='" + path + "' and version_id = '" + versionId + "'";
				} else {
					sql += "_RERUN_OBJECTS SET object_state = 3" + " WHERE path ='" + path + "' and version_id is null";
				}
			} else {
				if (versionId != null && !versionId.isEmpty()) {
					sql += "_OBJECTS SET object_state = 3" + " WHERE path ='" + path + "' and version_id = '" + versionId + "'";
				} else {
					sql += "_OBJECTS SET object_state = 3" + " WHERE path ='" + path + "' and version_id is null";
				}
			}
		} else {
			query1 = SQL_UPDATE_JOB_FAILED_OBJECTS;
			sql = UPDATE_JOB_ID + jobId;
			if (isRerun) {
				if (versionId != null && !versionId.isEmpty()) {
					sql += "_RERUN_OBJECTS SET object_state = 4" + " WHERE path ='" + path + "' and version_id = '" + versionId + "'";
				} else {
					sql += "_RERUN_OBJECTS SET object_state = 4" + " WHERE path ='" + path + "' and version_id is null";
				}
			} else {
				if (versionId != null && !versionId.isEmpty()) {
					sql += "_OBJECTS SET object_state = 4" + " WHERE path ='" + path + "' and version_id = '" + versionId + "'";
				} else {
					sql += "_OBJECTS SET object_state = 4" + " WHERE path ='" + path + "' and version_id is null";
				}
			}
		}

		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query1);
			 PreparedStatement pstmt2 = conn.prepareStatement(sql);
			) {
            int index = 1;
			pstmt.setObject(index++, size);
			pstmt.setObject(index++, jobId);
			pstmt.execute();
			pstmt2.execute();
			return true;
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return false;
	}

	@Override
	public boolean deleteRerunTableForDeletedObjects(String jobId) {
		String query = SQL_DELETE_JOB + jobId + "_RERUN_OBJECTS WHERE isdelete = 1 and object_state = 3";
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			pstmt.execute();
			return true;
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return false;
	}

	@Override
	public int getJobState(String jobId) {
		String query = SQL_GET_JOB_STATE + jobId;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			 ResultSet rs = pstmt.executeQuery();
			) {
			if (rs.next()) {
				return rs.getInt("job_state");
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return -1;
	}

	@Override
	public long insertMoveObject(String jobId, List<MoveData> list) {
		long totalSize = 0;
		String query = INSERT_JOB_ID + jobId + SQL_INSERT_MOVE_OBJECT;
		try (Connection conn = ds.getConnection();
			 PreparedStatement pstmt = conn.prepareStatement(query);
			) {
			conn.setAutoCommit(false);
			
			for (MoveData data: list) {
				pstmt.setObject(1, data.getPath());
				pstmt.setObject(2, data.getPath().charAt(data.getPath().length() - 1) != '/');
				pstmt.setObject(3, data.getmTime());
				pstmt.setObject(4, data.getSize());
				if (data.getETag() == null || data.getETag().isEmpty()) {
					pstmt.setObject(5, java.sql.Types.NULL);
				} else {
					pstmt.setObject(5, data.getETag());
				}
				pstmt.addBatch();
				totalSize += data.getSize();
			}
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
		return totalSize;
	}

}