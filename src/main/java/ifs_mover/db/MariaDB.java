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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import ifs_mover.Config;
import ifs_mover.IMOptions;
import ifs_mover.Utils;
import ifs_mover.IMOptions.WORK_TYPE;

public class MariaDB implements MoverDB {
    protected Logger logger;
	private static HikariConfig config = new HikariConfig();
	private static HikariDataSource ds;

	private static final int JOB_ERROR = 10;

	private static final String IFS_FILE = "file";
    private static final String SINGLE_QUOTATION = "'";
	private static final String UNDER_OBJECTS = "_OBJECTS";
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
			+ "`error_desc` VARCHAR(128),\n"
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
	
	private static final String SQL_UPDATE_JOB_OBJECTS = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_FAILED_OBJECTS = "UPDATE JOB SET failed_count = failed_count + 1, failed_size = failed_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_RERUN_SKIP = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ?, skip_objects_count = skip_objects_count + 1, skip_objects_size = skip_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_RERUN_OBJECTS = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_MOVED = "UPDATE JOB SET moved_objects_count = moved_objects_count + 1, moved_objects_size = moved_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_DELETED = "UPDATE JOB SET delete_objects_count = delete_objects_count + 1, delete_objects_size = delete_objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_MOVED_COUNT = "UPDATE JOB SET moved_objects_count = moved_objects_count + ?, moved_objects_size = moved_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_ERROR = "UPDATE JOB SET job_state = ?, error_desc = ? WHERE job_id = ";
	private static final String SQL_INSERT_JOB = "INSERT INTO JOB(pid, job_type, source_point, target_point, start) VALUES(?, ?, ?, ?, now())";
	private static final String SQL_UPDATE_JOB_START = "UPDATE JOB SET start = now() WHERE job_id =";
	private static final String SQL_UPDATE_JOB_END = "UPDATE JOB SET end = now() WHERE job_id =";
	private static final String SQL_INIT_JOB_RERUN = "UPDATE JOB SET objects_count = 0, objects_size = 0, moved_objects_count = 0, moved_objects_size = 0, failed_count = 0, failed_size = 0, skip_objects_count = 0, skip_objects_size = 0, delete_objects_count = 0, delete_objects_size = 0 WHERE job_id = ";
	private static final String SQL_INIT_MOVE_OBJECT_RERUN = "_OBJECTS SET skip_check = 0";
	private static final String SQL_INSERT_MOVE_OBJECT = "_OBJECTS (path, object_state, isfile, mtime, size, etag, multipart_info, tag) VALUES(?, 1, ?, ?, ?, ?, NULL, ?)";
	private static final String SQL_RERUN_INSERT_MOVE_OBJECT = "_OBJECTS (path, object_state, skip_check, isfile, mtime, size, etag, multipart_info, tag) VALUES(?, 1, 1, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_INSERT_MOVE_OBJECT_VERSIONING = "_OBJECTS (path, object_state, isfile, mtime, size, version_id, etag, multipart_info, tag, isdelete, islatest) VALUES(?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_RERUN_INSERT_MOVE_OBJECT_VERSIONING = "_OBJECTS (path, object_state, skip_check, isfile, mtime, size, version_id, etag, multipart_info, tag, isdelete, islatest) VALUES(?, 1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private static final String SQL_GET_MOVE_OBJECT_INFO = "SELECT path, isfile, size, object_state, mtime, version_id, etag, multipart_info, tag, isdelete, islatest, skip_check FROM JOB_";
	private static final String SQL_GET_MOVE_OBJECT_INFO_WHERE = "_OBJECTS WHERE isdelete = 0 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 0 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 0 and sequence > ";
	private static final String SQL_GET_MOVE_OBJECT_INFO_WHERE_DELETE = "_OBJECTS WHERE isdelete = 1 ";//"_OBJECTS WHERE object_state = 1 and isdelete = 1 and sequence > ";
	private static final String SQL_ORDER_BY_PATH = " ORDER BY path LIMIT ";
	private static final String SQL_SET_MOVE_OBJECT = "_OBJECTS SET object_state = 2 WHERE path = ? and version_id is null";
	private static final String SQL_SET_MOVE_OBJECT_VERSIONID = "_OBJECTS SET object_state = 2 WHERE path = ? and version_id = ?";
	private static final String SQL_SET_MOVE_OBJECT_COMPLETE = "_OBJECTS SET object_state = 3 WHERE path = ? and version_id is null";
	private static final String SQL_SET_MOVE_OBJECT_VERSIONID_COMPLETE = "_OBJECTS SET object_state = 3 WHERE path = ? and version_id = ?";
	private static final String SQL_SET_MOVE_OBJECT_FAILED = "_OBJECTS SET object_state = 4, error_date = now(), error_code = ?, error_desc = ? WHERE path = ? and version_id is null";
	private static final String SQL_SET_MOVE_OBJECT_VERSIONID_FAILED = "_OBJECTS SET object_state = 4, error_date = now(), error_code = ?, error_desc = ? WHERE path = ? and version_id = ?";
	private static final String SQL_GET_OBJECT_STATE = "SELECT object_state FROM JOB_";
	private static final String SQL_GET_OBJECT_INFO = "SELECT object_state, mtime, etag FROM JOB_";
	private static final String SQL_SET_MOVE_OBJECT_INFO = "_OBJECTS SET mtime = ?, size = ? WHERE path = '";
	private static final String SQL_GET_JOB_ID = "SELECT job_id FROM JOB WHERE pid = ";
	private static final String SQL_GET_JOB_TYPE = "SELECT job_type FROM JOB WHERE job_id = ";
	private static final String SQL_GET_MTIME = "SELECT mtime FROM JOB_";
	private static final String SQL_SET_PID = "UPDATE JOB SET pid = ";
	private static final String SQL_GET_PID = "SELECT pid FROM JOB WHERE job_id = ";
	private static final String SQL_GET_MAX_SEQUENCE = "SELECT MAX(sequence) FROM JOB_";
	private static final String SQL_DELETE_JOB = "DELETE FROM JOB_";
	private static final String SQL_DELETE_JOB_WHERE = "_OBJECTS WHERE skip_check = 0";
	private static final String SQL_SET_DELETE_OBJECT = "_OBJECTS SET object_state = 5 WHERE path = ? and version_id is null";
	private static final String SQL_SET_DELETE_OBJECT_VERSIONID = "_OBJECTS SET object_state = 5 WHERE path = ? and version_id = ?";

	private static final String SQL_DROP_MOVE_OBJECT = "DROP TABLE JOB_";
	private static final String SQL_DROP_MOVE_OBJECT_INDEX = "DROP INDEX IF EXISTS idx_path ON JOB_";

	private static final String SQL_OBJECT_WHERE_PATH = "_OBJECTS WHERE path = '";
	private static final String SQL_SKIP_CHECK = "_OBJECTS SET object_state = 1, skip_check = 1, mtime = '";
	private static final String SQL_SKIP_CHECK_WHERE_PATH = "_OBJECTS SET skip_check = 1 WHERE path = '";
	private static final String SQL_SIZE = "', size = ";
	private static final String SQL_WHERE_PATH = " WHERE path = '";
	private static final String SQL_VERSIONID_IS_NULL = "' and version_id is null";
	private static final String SQL_VERSIONID = "' and version_id = '";

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
		String jdbcUrl = "jdbc:mariadb://" + dbUrl + ":" + dbPort + "/" + dbName + "?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf8";

		config.setJdbcUrl(jdbcUrl);
		config.setUsername(userName);
		config.setPassword(passwd);
		config.setDriverClassName("org.mariadb.jdbc.Driver");
		config.setConnectionTestQuery("select 1");
		config.addDataSourceProperty("maxPoolSize" , poolSize );
		config.addDataSourceProperty("minPoolSize" , poolSize );
		config.setPoolName("ifsmover");
		config.setMaximumPoolSize(poolSize);
		config.setMinimumIdle(poolSize);

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

            // logger.debug(pstmt.toString());
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

			// logger.debug(pstmt.toString());
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
		String query = "CREATE TABLE IF NOT EXISTS JOB_" + jobId + "_OBJECTS (\n"
				+ "`sequence` BIGINT AUTO_INCREMENT,"
				+ "`path` VARBINARY(2048) NOT NULL,\n"
				+ "`size` BIGINT NOT NULL,\n"
				+ "`object_state` INT NOT NULL DEFAULT 0,\n"
				+ "`isfile`	BOOLEAN default true,\n"
				+ "`skip_check` BOOLEAN DEFAULT false,\n"
				+ "`mtime` TEXT NOT NULL,\n"
				+ "`version_id` VARCHAR(64),\n"
				+ "`etag` VARCHAR(64),\n"
				+ "`multipart_info` TEXT DEFAULT NULL,\n"
				+ "`tag` TEXT DEFAULT NULL,\n"
				+ "`isdelete` BOOLEAN DEFAULT false,\n"
				+ "`islatest` BOOLEAN DEFAULT false,\n"
				+ "`error_date` TEXT,\n"
				+ "`error_code` TEXT,\n"
				+ "`error_desc` TEXT,\n"
				+ "PRIMARY KEY(`sequence`), INDEX idx_path(`path`))ENGINE=InnoDB DEFAULT CHARSET=utf8;";
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
		String sql = SQL_UPDATE_JOB_ERROR + jobId;
		List<Object> params = new ArrayList<Object>();
		params.add(JOB_ERROR);
		params.add(msg);

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
		if (tag == null || tag.isEmpty()) {
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
		if (versionId == null || versionId.isEmpty()) {
			sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SQL_VERSIONID_IS_NULL;
		} else {
			sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		}

		List<HashMap<String, Object>> resultList = null;

		try {
			resultList = select(sql, null);
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
		String sql;
		sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SINGLE_QUOTATION;

		List<HashMap<String, Object>> resultList = null;

		try {
			resultList = select(sql, null);
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
		String sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK_WHERE_PATH + path + SINGLE_QUOTATION;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateRerunSkipObjectVersion(String jobId, String path, String versionId, boolean isLatest) {
		String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET skip_check = 1, islatest = ";
		if (isLatest) {
			sql += "1";
		} else {
			sql += "0";
		}

		if (versionId == null || versionId.isEmpty()) {
			sql += " WHERE path = '" + path + SQL_VERSIONID_IS_NULL;
		} else {
			sql += " WHERE path = '" + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		}

		try {
			execute(sql, null);
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
		String sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SINGLE_QUOTATION;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateObjectMove(String jobId, String path, String versionId) {		
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 2 WHERE path = '" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		} else {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 2 WHERE path = '" + path + "' and version_id is null";
		}

		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateToMoveObjectVersion(String jobId, String mTime, long size, String path, String versionId) {
		String sql;
		if (versionId == null || versionId.isEmpty()) { 
			sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SQL_VERSIONID_IS_NULL;
		} else {
			sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		}

		try {
			execute(sql, null);
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
		String sql = UPDATE_JOB_ID + jobId;
		if (versionId != null && !versionId.isEmpty()) {
			sql += "_OBJECTS SET object_state = 3 WHERE path ='" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		} else {
			sql += "_OBJECTS SET object_state = 3 WHERE path ='" + path + "' and version_id is null";
		}

		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean updateObjectMoveEventFailed(String jobId, String path, String versionId, String errorCode, String errorMessage) {
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 4, error_date = now(), error_code ='" + errorCode + "', error_desc = '" + errorMessage + "' WHERE path = '" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		} else {
			sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 4, error_date = now(), error_code ='" + errorCode + "', error_desc = '" + errorMessage + "' WHERE path = '" + path + "' and version_id is null";
		}

		try {
			execute(sql, null);
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
	public List<HashMap<String, Object>> getToMoveObjectsInfo(String jobId, long sequence, long limit) {
		List<HashMap<String, Object>> resultList = null;

		String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
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

		String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE_DELETE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
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
		String sql;
		if (versionId != null && !versionId.isEmpty()) {
			sql = SQL_DELETE_JOB + jobId + "_OBJECTS WHERE path = '" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		} else {
			sql = SQL_DELETE_JOB + jobId + "_OBJECTS WHERE path = '" + path + "' and version_id is null";
		}

		try {
			execute(sql, null);
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
		String sql = SQL_DROP_MOVE_OBJECT_INDEX + jobId + UNDER_OBJECTS;
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
		String sql = SQL_GET_OBJECT_STATE + jobId + SQL_OBJECT_WHERE_PATH + path + SINGLE_QUOTATION;

		try {
			resultList = select(sql, null);
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
		String sql = SQL_GET_MTIME + jobId + SQL_OBJECT_WHERE_PATH + path + SINGLE_QUOTATION;

		try {
			resultList = select(sql, null);
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
		String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 3 WHERE path ='" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		try {
			execute(sql, null);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
}
