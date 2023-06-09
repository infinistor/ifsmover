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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.VersionListing;

import ifs_mover.Config;
import ifs_mover.IMOptions;
import ifs_mover.MoveData;
import ifs_mover.IMOptions.WORK_TYPE;

public class SqliteDB implements MoverDB {
	protected static Logger logger;
	
	private static Connection con;
	private static final String IFS_FILE = "file";
	private static final String JDBC = "org.sqlite.JDBC";
	private static final String DB_FILE_URL = "jdbc:sqlite:ifs-mover.db";
	
	private static final int JOB_ERROR = 10;
	private static final int CACHE_SIZE = 100000;
	private static final int WAIT_TIMEOUT = 60000;
	
	private static final String SINGLE_QUOTATION = "'";
	private static final String UNDER_OBJECTS = "_OBJECTS";
	private static final String UNDER_INDEX = "_INDEX";
	private static final String WHERE_JOB_ID = " WHERE job_id = ";

	private static final String CREATE_JOB_TABLE =
			"CREATE TABLE IF NOT EXISTS 'JOB' (\n"
			+ "'job_id' INTEGER NOT NULL,\n"
			+ "'job_state' INTEGER NOT NULL DEFAULT 0,\n"
			+ "'pid' INTEGER NOT NULL,\n"
			+ "'job_type' TEXT NOT NULL,\n"
			+ "'source_point' TEXT NOT NULL,\n"
			+ "'target_point' TEXT NOT NULL, \n"
			+ "'objects_count' INTEGER DEFAULT 0,\n"
			+ "'objects_size' INTEGER DEFAULT 0,\n"
			+ "'moved_objects_count' INTEGER DEFAULT 0,\n"
			+ "'moved_objects_size' INTEGER DEFAULT 0,\n"
			+ "'failed_count' INTEGER DEFAULT 0,\n"
			+ "'failed_size' INTEGER DEFAULT 0,\n"
			+ "'skip_objects_count' INTEGER DEFAULT 0,\n"
			+ "'skip_objects_size' INTEGER DEFAULT 0,\n"
			+ "'delete_objects_count' INTEGER DEFAULT 0,\n"
			+ "'delete_objects_size' INTEGER DEFAULT 0,\n"
			+ "'start' TEXT,\n"
			+ "'end' TEXT,\n"
			+ "'error_desc' TEXT,\n"
			+ "PRIMARY KEY('job_id' AUTOINCREMENT));";
	
	private static final String UPDATE_JOB_ID = "UPDATE JOB_";
	private static final String INSERT_JOB_ID = "INSERT INTO JOB_";
	private static final String SQL_UPDATE_JOB_STATE_MOVE = "UPDATE JOB SET job_state = 1 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_COMPLETE = "UPDATE JOB SET job_state = 4 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_STOP = "UPDATE JOB SET job_state = 5 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_REMOVE = "UPDATE JOB SET job_state = 6 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_RERUN = "UPDATE JOB SET job_state = 7 WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_STATE_RERUN_MOVE = "UPDATE JOB SET job_state = 8 WHERE job_id = ?";
	private static final String SQL_SELECT_JOB_STATUS = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB ORDER BY job_id";
	private static final String SQL_SELECT_JOB_STATUS_JOBID = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB WHERE job_id = ?";
	private static final String SQL_SELECT_JOB_STATUS_SRC_BUCKET = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB WHERE source_point LIKE ? ORDER BY job_id";
	private static final String SQL_SELECT_JOB_STATUS_DST_BUCKET = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB WHERE target_point LIKE ? ORDER BY job_id";
	private static final String SQL_SELECT_JOB_STATUS_SRC_DST_BUCKET = "SELECT job_id, job_state, job_type, source_point, target_point, objects_count, objects_size, moved_objects_count, moved_objects_size, failed_count, failed_size, skip_objects_count, skip_objects_size, delete_objects_count, delete_objects_size, start, end, error_desc FROM JOB WHERE source_point LIKE ? AND target_point LIKE ? ORDER BY job_id";
	
	private static final String SQL_UPDATE_JOB_OBJECTS = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_FAILED_OBJECTS = "UPDATE JOB SET failed_count = failed_count + 1, failed_size = failed_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_RERUN_SKIP = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ?, skip_objects_count = skip_objects_count + 1, skip_objects_size = skip_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_SKIP = "UPDATE JOB SET skip_objects_count = skip_objects_count + 1, skip_objects_size = skip_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_RERUN_OBJECTS = "UPDATE JOB SET objects_count = objects_count + 1, objects_size = objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_MOVED = "UPDATE JOB SET moved_objects_count = moved_objects_count + 1, moved_objects_size = moved_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_DELETED = "UPDATE JOB SET delete_objects_count = delete_objects_count + 1, delete_objects_size = delete_objects_size + ? WHERE job_id =  ?";
	private static final String SQL_UPDATE_JOB_MOVED_COUNT = "UPDATE JOB SET moved_objects_count = moved_objects_count + ?, moved_objects_size = moved_objects_size + ? WHERE job_id = ?";
	private static final String SQL_UPDATE_JOB_ERROR = "UPDATE JOB SET job_state = ?, error_desc = ? WHERE job_id = ";
	private static final String SQL_INSERT_JOB = "INSERT INTO JOB(pid, job_type, source_point, target_point, start) VALUES(?, ?, ?, ?, datetime('now', 'localtime'))";
	private static final String SQL_UPDATE_JOB_START = "UPDATE JOB SET start = datetime('now', 'localtime') WHERE job_id =";
	private static final String SQL_UPDATE_JOB_END = "UPDATE JOB SET end = datetime('now', 'localtime') WHERE job_id =";
	private static final String SQL_INIT_JOB_RERUN = "UPDATE JOB SET objects_count = 0, objects_size = 0, moved_objects_count = 0, moved_objects_size = 0, failed_count = 0, failed_size = 0, skip_objects_count = 0, skip_objects_size = 0, delete_objects_count = 0, delete_objects_size = 0 WHERE job_id = ";
	private static final String SQL_INIT_MOVE_OBJECT_RERUN = "_OBJECTS SET skip_check = 0";
	private static final String SQL_INSERT_MOVE_OBJECT = "_OBJECTS (path, object_state, isfile, mtime, size, etag, multipart_info, tag) VALUES(?, 1, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_REPLACE = "REPLACE INTO JOB_";
	private static final String SQL_INSERT_TARGET_OBJECT = "_TARGET_OBJECTS (path, version_id, size, etag) VALUES(?, ?, ?, ?)";
	private static final String SQL_RERUN_INSERT_MOVE_OBJECT = "_OBJECTS (path, object_state, skip_check, isfile, mtime, size, etag, multipart_info, tag) VALUES(?, 1, 1, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_INSERT_MOVE_OBJECT_VERSIONING = "_OBJECTS (path, object_state, isfile, mtime, size, version_id, etag, multipart_info, tag, isdelete, islatest) VALUES(?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_RERUN_INSERT_MOVE_OBJECT_VERSIONING = "_OBJECTS (path, object_state, skip_check, isfile, mtime, size, version_id, etag, multipart_info, tag, isdelete, islatest) VALUES(?, 1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private static final String SQL_GET_MOVE_OBJECT_INFO = "SELECT path, isfile, size, object_state, mtime, version_id, etag, multipart_info, tag, isdelete, islatest, skip_check FROM JOB_";
	private static final String SQL_GET_MOVE_OBJECT_INFO_WHERE = "_OBJECTS WHERE isdelete = 0 ";
	private static final String SQL_GET_MOVE_OBJECT_INFO_WHERE_DELETE = "_OBJECTS WHERE isdelete = 1 ";
	private static final String SQL_ORDER_BY_PATH = " ORDER BY path LIMIT ";
	private static final String SQL_SET_MOVE_OBJECT = "_OBJECTS SET object_state = 2 WHERE path = ? and version_id is null";
	private static final String SQL_SET_MOVE_OBJECT_VERSIONID = "_OBJECTS SET object_state = 2 WHERE path = ? and version_id = ?";
	private static final String SQL_SET_MOVE_OBJECT_COMPLETE = "_OBJECTS SET object_state = 3 WHERE path = ? and version_id is null";
	private static final String SQL_SET_MOVE_OBJECT_VERSIONID_COMPLETE = "_OBJECTS SET object_state = 3 WHERE path = ? and version_id = ?";
	private static final String SQL_SET_MOVE_OBJECT_FAILED = "_OBJECTS SET object_state = 4, error_date = datetime('now', 'localtime'), error_code = ?, error_desc = ? WHERE path = ? and version_id is null";
	private static final String SQL_SET_MOVE_OBJECT_VERSIONID_FAILED = "_OBJECTS SET object_state = 4, error_date = datetime('now', 'localtime'), error_code = ?, error_desc = ? WHERE path = ? and version_id = ?";
	private static final String SQL_GET_OBJECT_STATE = "SELECT object_state FROM JOB_";
	private static final String SQL_GET_OBJECT_INFO = "SELECT object_state, mtime, etag FROM JOB_";
	private static final String SQL_SET_MOVE_OBJECT_INFO = "_OBJECTS SET mtime = ?, size = ? WHERE path = ?";
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
	private static final String SQL_DROP_MOVE_OBJECT_INDEX = "DROP INDEX IF EXISTS PATH_";

	// private static final String SQL_OBJECT_WHERE_PATH = "_OBJECTS WHERE path = '";
	private static final String SQL_OBJECT_WHERE_PATH = "_OBJECTS WHERE path = ?";
	private static final String SQL_OBJECT_WHERE_PATH_WITH_VERSIONID = "_OBJECTS WHERE path = ? and version_id = ?";
	private static final String SQL_OBJECT_WHERE_PATH_WITH_VERSIONID_NULL = "_OBJECTS WHERE path = ? and version_id is null";
	private static final String SQL_SKIP_CHECK = "_OBJECTS SET object_state = 1, skip_check = 1, mtime = '";
	private static final String SQL_SKIP_CHECK_WHERE_PATH = "_OBJECTS SET object_state = 3, skip_check = 1 WHERE path = ?";
	private static final String SQL_SKIP_CHECK_WHERE_PATH_WITH_VERSION = "_OBJECTS SET object_state = 3, skip_check = 1 WHERE path = ? and version_id = ?";
	private static final String SQL_SKIP_CHECK_WHERE_PATH_WITH_VERSION_NULL = "_OBJECTS SET object_state = 3, skip_check = 1 WHERE path = ? and version_id is null";
	private static final String SQL_SIZE = "', size = ";
	// private static final String SQL_WHERE_PATH = " WHERE path = '";
	private static final String SQL_VERSIONID_IS_NULL = "' and version_id is null";
	private static final String SQL_VERSIONID = "' and version_id = '";
	private static final String SQL_WHERE_PATH = " WHERE path = ?";
	private static final String SQL_WHERE_PATH_WITH_VERSIONID = " WHERE path = ? and version_id = ?";
	private static final String SQL_WHERE_PATH_WITH_VERSIONID_NULL = " WHERE path = ? and version_id is null";


	private static final String SQL_GET_TARGET = "SELECT path FROM JOB_";
	private static final String SQL_GET_TARGET_OBJECT_PATH = "_TARGET_OBJECTS WHERE path = ?";
	private static final String SQL_GET_TARGET_OBJECT_PATH_WITH_SIZE = "_TARGET_OBJECTS WHERE path = ? and size = ?";
	private static final String SQL_GET_TARGET_OBJECT_PATH_WITH_ETAG = "_TARGET_OBJECTS WHERE path = ? and etag = ?";

	private SqliteDB() {
		logger = LoggerFactory.getLogger(SqliteDB.class);
	}

    public static SqliteDB getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
        private static final SqliteDB INSTANCE = new SqliteDB();
    }
	
	@Override
	public void init(String dbUrl, String dbPort, String dbName, String userName, String passwd, int poolSize)
			throws Exception {
		// TODO Auto-generated method stub
		init();
		createJobTable();
	}

	private static void init() {
		try {
			Class.forName(JDBC);
			SQLiteConfig config = new SQLiteConfig();
			config.setCacheSize(CACHE_SIZE);
			config.setBusyTimeout(WAIT_TIMEOUT);
			config.setTransactionMode(SQLiteConfig.TransactionMode.DEFERRED);
    		config.setLockingMode(SQLiteConfig.LockingMode.NORMAL);
    		config.setSynchronous(SQLiteConfig.SynchronousMode.FULL);
    		config.setJournalMode(SQLiteConfig.JournalMode.WAL);
			config.setEncoding(SQLiteConfig.Encoding.UTF_8);
			SqliteDB.con = DriverManager.getConnection(DB_FILE_URL, config.toProperties());
		} catch (SQLException | ClassNotFoundException e) {
			logger.error(e.getMessage());
		}
	}
	
	private static Connection getReadConnection() {
		Connection con = null;
		try {
			Class.forName(JDBC);
			SQLiteConfig config = new SQLiteConfig();
			config.setReadOnly(true);
			con = DriverManager.getConnection(DB_FILE_URL, config.toProperties());
		} catch (SQLException | ClassNotFoundException e) {
			logger.error(e.getMessage());
		}

		return con;
	}

	public static void open() {
		if (SqliteDB.con == null) {
			try {
				Class.forName(JDBC);
				SqliteDB.con = DriverManager.getConnection(DB_FILE_URL);
			} catch (SQLException | ClassNotFoundException e) {
				logger.error(e.getMessage());
			}
		}
	}

	public static void close() {
		if (SqliteDB.con != null) {
			try {
				SqliteDB.con.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
			SqliteDB.con = null;
		}
	}
	
	public static Connection getConnection() {
		open();
		return SqliteDB.con;
	}
	
	public static void createJobTable() {
		open();
		try (Statement stmt = con.createStatement()) {
			stmt.execute(CREATE_JOB_TABLE);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}
	
	@Override
	public void createJob(String pid, String select, Config sourceConfig, Config targetConfig) {
		open();
		try(PreparedStatement pstmt = con.prepareStatement(SQL_INSERT_JOB);) {
			pstmt.setInt(1, Integer.parseInt(pid));
			pstmt.setString(2, select);
			if (IFS_FILE.compareToIgnoreCase(select) == 0) {
				pstmt.setString(3, sourceConfig.getMountPoint() + sourceConfig.getPrefix());
			} else {
				pstmt.setString(3, sourceConfig.getBucket());
			}
			pstmt.setString(4, targetConfig.getBucket());
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}
	
	@Override
	public String getJobId(String pid) {
		String jobId = "";
		open();
		final String sql = SQL_GET_JOB_ID + pid;
		try (Statement stmt = con.createStatement();
			 ResultSet rs = stmt.executeQuery(sql);) {
			if (rs.next()) {
				jobId = rs.getString(1);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 

		return jobId;
	}

	@Override
	public void createMoveObjectTable(String jobId) {
		open();
		String sql = "CREATE TABLE IF NOT EXISTS JOB_" + jobId + "_OBJECTS (\n"
				+ "'sequence' INTEGER PRIMARY KEY AUTOINCREMENT,"
				+ "'path' TEXT NOT NULL,\n"
				+ "'size' INTEGER NOT NULL,\n"
				+ "'object_state' INTEGER NOT NULL DEFAULT 0,\n"
				+ "'isfile'	INTEGER NOT NULL,\n"
				+ "'skip_check' INTEGER DEFAULT 0,\n"
				+ "'mtime' TEXT NOT NULL,\n"
				+ "'version_id' TEXT,\n"
				+ "'etag' TEXT,\n"
				+ "'multipart_info' TEXT,\n"
				+ "'tag' TEXT,\n"
				+ "'isdelete' INTEGER DEFAULT 0,\n"
				+ "'islatest' INTEGER DEFAULT 0,\n"
				+ "'error_date' TEXT,\n"
				+ "'error_code' TEXT,\n"
				+ "'error_desc' TEXT,\n"
				+ "UNIQUE('sequence', 'path'))";
		try(Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
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
		open();
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, jobId);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public void insertErrorJob(String jobId, String msg) {
		open();
		try (PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_ERROR + jobId)) {
			pstmt.setInt(1, JOB_ERROR);
			pstmt.setString(2, msg);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public boolean insertMoveObject(String jobId, boolean isFile, String mTime, long size, String path, String etag, String tag) {
		open();
		String sql = INSERT_JOB_ID + jobId + SQL_INSERT_MOVE_OBJECT;
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (isFile) {
				pstmt.setInt(2, 1);
			} else {
				pstmt.setInt(2, 0);
			}
			pstmt.setString(3, mTime);
			pstmt.setLong(4, size);
			if (etag == null || etag.isEmpty()) {
				pstmt.setNull(5, java.sql.Types.NULL);
			} else {
				pstmt.setString(5, etag);
			}
			if (tag == null || tag.isEmpty()) {
				pstmt.setNull(6, java.sql.Types.NULL);
			} else {
				pstmt.setString(6, tag);
			}

			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean insertMoveObjectVersioning(String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest) {
		open();
		String sql = INSERT_JOB_ID + jobId + SQL_INSERT_MOVE_OBJECT_VERSIONING;
		try  (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			
			if (isFile) {
				pstmt.setInt(2, 1);
			} else {
				pstmt.setInt(2, 0);
			}
			
			pstmt.setString(3, mTime);
			pstmt.setLong(4, size);
			
			if (versionId == null || versionId.isEmpty()) {
				pstmt.setNull(5, java.sql.Types.NULL);
			} else {
				pstmt.setString(5, versionId);
			}
			
			if (etag == null || etag.isEmpty()) {
				pstmt.setNull(6, java.sql.Types.NULL);
			} else {
				pstmt.setString(6, etag);
			}
			
			if (multipartInfo == null || multipartInfo.isEmpty()) {
				pstmt.setNull(7, java.sql.Types.NULL);
			} else {
				pstmt.setString(7, multipartInfo);
			}

			if (tag == null || tag.isEmpty()) {
				pstmt.setNull(8, java.sql.Types.NULL);
			} else {
				pstmt.setString(8, tag);
			}

			if (isDelete) {
				pstmt.setInt(9, 1);
			} else {
				pstmt.setInt(9, 0);
			}
			
			if (isLatest) {
				pstmt.setInt(10, 1);
			} else {
				pstmt.setInt(10, 0);
			}
			
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}
	
	@Override
	public boolean updateJobInfo(String jobId, long size) {
		open();
		try(PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_OBJECTS)) {
			pstmt.setLong(1, size);
			pstmt.setString(2, jobId);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean updateJobRerunInfo(String jobId, long size) {
		open();
		try(PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_RERUN_OBJECTS)) {
			pstmt.setLong(1, size);
			pstmt.setString(2, jobId);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean insertRerunMoveObject(String jobId, boolean isFile, String mTime, long size, String path, String etag, String multipartInfo, String tag) {
		open();
		String sql = INSERT_JOB_ID + jobId + SQL_RERUN_INSERT_MOVE_OBJECT;
		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (isFile) {
				pstmt.setInt(2, 1);
			} else {
				pstmt.setInt(2, 0);
			}
			pstmt.setString(3, mTime);
			pstmt.setLong(4, size);
			if (etag == null || etag.isEmpty()) {
				pstmt.setNull(5, java.sql.Types.NULL);
			} else {
				pstmt.setString(5, etag);
			}
			
			if (multipartInfo == null || multipartInfo.isEmpty()) {
				pstmt.setNull(6, java.sql.Types.NULL);
			} else {
				pstmt.setString(6, multipartInfo);
			}

			if (tag == null || tag.isEmpty()) {
				pstmt.setNull(7, java.sql.Types.NULL);
			} else {
				pstmt.setString(7, tag);
			}
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean insertRerunMoveObjectVersion(String jobId, boolean isFile, String mTime, long size, String path, String versionId, String etag, String multipartInfo, String tag, boolean isDelete, boolean isLatest) {
		open();
		String sql = INSERT_JOB_ID + jobId + SQL_RERUN_INSERT_MOVE_OBJECT_VERSIONING;
		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			
			if (isFile) {
				pstmt.setInt(2, 1);
			} else {
				pstmt.setInt(2, 0);
			}
			
			pstmt.setString(3, mTime);
			pstmt.setLong(4, size);
			
			if (versionId == null || versionId.isEmpty()) {
				pstmt.setNull(5, java.sql.Types.NULL);
			} else {
				pstmt.setString(5, versionId);
			}

			if (etag == null || etag.isEmpty()) {
				pstmt.setNull(6, java.sql.Types.NULL);
			} else {
				pstmt.setString(6, etag);
			}
			
			if (multipartInfo == null || multipartInfo.isEmpty()) {
				pstmt.setNull(7, java.sql.Types.NULL);
			} else {
				pstmt.setString(7, multipartInfo);
			}

			if (tag == null || tag.isEmpty()) {
				pstmt.setNull(8, java.sql.Types.NULL);
			} else {
				pstmt.setString(8, tag);
			}

			if (isDelete) {
				pstmt.setInt(9, 1);
			} else { 
				pstmt.setInt(9, 0);
			}
			
			if (isLatest) {
				pstmt.setInt(10, 1);
			} else { 
				pstmt.setInt(10, 0);
			}
			
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public Map<String, String> infoExistObjectVersion(String jobId, String path, String versionId) {
		open();
		Map<String, String> info = new HashMap<String, String>();	
		String sql;
		if (versionId == null || versionId.isEmpty()) {
			sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH_WITH_VERSIONID_NULL;
		} else {
			sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH_WITH_VERSIONID;
		}

		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(2, versionId);
			}
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				info.put(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE, rs.getString(1));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_MTIME, rs.getString(2));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_ETAG, rs.getString(3));
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		// if (versionId == null || versionId.isEmpty()) {
		// 	sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SQL_VERSIONID_IS_NULL;
		// } else {
		// 	sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		// }

		// try (Statement stmt = con.createStatement();
		// 	ResultSet rs = stmt.executeQuery(sql);) {
		// 	if (rs.next()) {
		// 		info.put(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE, rs.getString(1));
		// 		info.put(MOVE_OBJECTS_TABLE_COLUMN_MTIME, rs.getString(2));
		// 		info.put(MOVE_OBJECTS_TABLE_COLUMN_ETAG, rs.getString(3));
		// 	} 
		// } catch (SQLException e) {
		// 	logger.error(e.getMessage());
		// } 
		return info;
	}

	@Override
	public Map<String, String> infoExistObject(String jobId, String path) {
		open();
		Map<String, String> info = new HashMap<String, String>();	
		String sql;
		sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH;

		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				info.put(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE, rs.getString(1));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_MTIME, rs.getString(2));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_ETAG, rs.getString(3));
			} 
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 
		// sql = SQL_GET_OBJECT_INFO + jobId + SQL_OBJECT_WHERE_PATH + path + SINGLE_QUOTATION;

		// try (Statement stmt = con.createStatement();
		// 	ResultSet rs = stmt.executeQuery(sql);) {
		// 	if (rs.next()) {
		// 		info.put(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE, rs.getString(1));
		// 		info.put(MOVE_OBJECTS_TABLE_COLUMN_MTIME, rs.getString(2));
		// 		info.put(MOVE_OBJECTS_TABLE_COLUMN_ETAG, rs.getString(3));
		// 	} 
		// } catch (SQLException e) {
		// 	logger.error(e.getMessage());
		// } 
		return info;
	}

	@Override
	public boolean updateRerunSkipObject(String jobId, String path) {
		open();
		String sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK_WHERE_PATH;
		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 
		// try(Statement stmt = con.createStatement()) {
		// 	if (stmt.executeUpdate(sql) == 1) {
		// 		return true;
		// 	}
		// } catch (SQLException e) {
		// 	logger.warn(e.getMessage());
		// } 

		return false;
	}

	@Override
	public boolean updateRerunSkipObjectVersion(String jobId, String path, String versionId, boolean isLatest) {
		open();
		String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET skip_check = 1, islatest = ";
		if (isLatest) {
			sql += "1";
		} else {
			sql += "0";
		}

		if (versionId == null || versionId.isEmpty()) {
			sql += SQL_WHERE_PATH_WITH_VERSIONID_NULL;
		} else {
			sql += SQL_WHERE_PATH_WITH_VERSIONID;
		}
		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(2, versionId);
			}
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		}

		// if (versionId == null || versionId.isEmpty()) {
		// 	sql += " WHERE path = '" + path + SQL_VERSIONID_IS_NULL;
		// } else {
		// 	sql += " WHERE path = '" + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		// }

		// try(Statement stmt = con.createStatement()) {
		// 	if (stmt.executeUpdate(sql) == 1) {
		// 		return true;
		// 	}
		// } catch (SQLException e) {
		// 	logger.warn(e.getMessage());
		// } 

		return false;
	}

	@Override
	public boolean updateJobRerunSkipInfo(String jobId, long size) {
		open();
		try(PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_RERUN_SKIP)) {
			pstmt.setLong(1, size);
			pstmt.setLong(2, size);
			pstmt.setString(3, jobId);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean updateToMoveObject(String jobId, String mTime, long size, String path) {
		open();
		String sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH;
		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 
		// String sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SINGLE_QUOTATION;
		// try (Statement stmt = con.createStatement()) {
		// 	if (stmt.executeUpdate(sql) == 1) {
		// 		return true;
		// 	}
		// } catch (SQLException e) {
		// 	logger.warn(e.getMessage());
		// } 

		return false;
	}

	@Override
	public boolean updateObjectMove(String jobId, String path, String versionId) {
		open();
		String sql;
		if (versionId == null || versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT;
		} else {
			sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_VERSIONID;
		}

		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(2, versionId);
			} 
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean updateToMoveObjectVersion(String jobId, String mTime, long size, String path, String versionId) {
		open();
		String sql;
		if (versionId == null || versionId.isEmpty()) { 
			sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH_WITH_VERSIONID_NULL;
		} else {
			sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH_WITH_VERSIONID;
		}
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(2, versionId);
			} 
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 
		// if (versionId == null || versionId.isEmpty()) { 
		// 	sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SQL_VERSIONID_IS_NULL;
		// } else {
		// 	sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK + mTime + SQL_SIZE + size + SQL_WHERE_PATH + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		// }

		// try (Statement stmt = con.createStatement()) {
		// 	if (stmt.executeUpdate(sql) == 1) {
		// 		return true;
		// 	}
		// } catch (SQLException e) {
		// 	logger.warn(e.getMessage());
		// } 

		return false;
	}

	@Override
	public boolean updateJobMoved(String jobId, long size) {
		open();
		try (PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_MOVED)) {
			pstmt.setLong(1, size);
			pstmt.setString(2, jobId);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 
		return false;
	}

	@Override
	public boolean updateObjectMoveComplete(String jobId, String path, String versionId) {
		open();
		String sql;
		if (versionId == null || versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_COMPLETE;
		} else {
			sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_VERSIONID_COMPLETE;
		}
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(2, versionId);
			} 
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean updateObjectMoveEventFailed(String jobId, String path, String versionId, String errorCode, String errorMessage) {
		open();
		String sql;
		if (versionId == null || versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_FAILED;
		} else {
			sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_VERSIONID_FAILED;
		}

		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, errorCode);
			pstmt.setString(2, errorMessage);
			pstmt.setString(3, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(4, versionId);
			} 
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 

		return false;
	}
	
	@Override
	public boolean updateJobFailedInfo(String jobId, long size) {
		open();
		try(PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_FAILED_OBJECTS)) {
			pstmt.setLong(1, size);
			pstmt.setString(2, jobId);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}
	
	@Override
	public void deleteCheckObjects(String jobId) {
		open();
		// String sql = SQL_DELETE_JOB + jobId + SQL_DELETE_JOB_WHERE;
		String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 1 WHERE skip_check = 0";
		try (Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}
	
	@Override
	public long getMaxSequence(String jobId) {
		open();
		long maxSequence = 0;
		String sql = SQL_GET_MAX_SEQUENCE + jobId + UNDER_OBJECTS;
		try (Statement stmt = con.createStatement();
			 ResultSet rs = stmt.executeQuery(sql);) {
			if (rs.next()) {
				maxSequence = rs.getLong(1);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 

		return maxSequence;
	}

	@Override
	public List<MoveData> getToMoveObjectsInfo(String jobId, long sequence, long limit) {
		open();
		String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
		List<MoveData> list = new ArrayList<MoveData>();
		/*
		try (Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);) {
	   		while (rs.next()) {
				HashMap<String, Object> info = new HashMap<String, Object>();
		   		info.put(MOVE_OBJECTS_TABLE_COLUMN_PATH, rs.getString(1));
				String temp = rs.getString(2);
				byte isFile = Byte.parseByte(temp);
		   		info.put(MOVE_OBJECTS_TABLE_COLUMN_ISFILE, isFile);
		   		info.put(MOVE_OBJECTS_TABLE_COLUMN_SIZE, rs.getLong(3));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE, rs.getInt(4));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_MTIME, rs.getString(5));
		   		info.put(MOVE_OBJECTS_TABLE_COLUMN_VERSIONID, rs.getString(6));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_ETAG, rs.getString(7));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_MULTIPART_INFO, rs.getString(8));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_TAG, rs.getString(9));
		   		info.put(MOVE_OBJECTS_TABLE_COLUMN_ISDELETE, rs.getByte(10));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_ISLATEST, rs.getByte(11));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_SKIP_CHECK, rs.getByte(12));
				
		   		list.add(info);
			}
	   	} catch (SQLException e) {
	   		logger.error(e.getMessage());
   		} 

		if (list.size() == 0) {
			return null;
		} 
		*/
   		return list;
	}

	@Override
	public List<HashMap<String, Object>> getToDeleteObjectsInfo(String jobId, long sequence, long limit) {
		open();
		String sql = SQL_GET_MOVE_OBJECT_INFO + jobId + SQL_GET_MOVE_OBJECT_INFO_WHERE_DELETE + SQL_ORDER_BY_PATH + sequence + ", " + limit;
		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		try (Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);) {
	   		while (rs.next()) {
				HashMap<String, Object> info = new HashMap<String, Object>();
		   
				info.put(MOVE_OBJECTS_TABLE_COLUMN_PATH, rs.getString(1));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_ISFILE, rs.getByte(2));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_SIZE, rs.getLong(3));
			 	info.put(MOVE_OBJECTS_TABLE_COLUMN_OBJECT_STATE, rs.getInt(4));
			 	info.put(MOVE_OBJECTS_TABLE_COLUMN_MTIME, rs.getString(5));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_VERSIONID, rs.getString(6));
			 	info.put(MOVE_OBJECTS_TABLE_COLUMN_ETAG, rs.getString(7));
			 	info.put(MOVE_OBJECTS_TABLE_COLUMN_MULTIPART_INFO, rs.getString(8));
			 	info.put(MOVE_OBJECTS_TABLE_COLUMN_TAG, rs.getString(9));
				info.put(MOVE_OBJECTS_TABLE_COLUMN_ISDELETE, rs.getByte(10));
			 	info.put(MOVE_OBJECTS_TABLE_COLUMN_ISLATEST, rs.getByte(11));
			 	info.put(MOVE_OBJECTS_TABLE_COLUMN_SKIP_CHECK, rs.getByte(12));
				
		   		list.add(info);
			}
	   	} catch (SQLException e) {
	   		logger.error(e.getMessage());
   		} 
   
		if (list.size() == 0) {
			return null;
		}

   		return list;
	}

	@Override
	public boolean updateJobDeleted(String jobId, long size) {
		open();
		try (PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_DELETED)) {
			pstmt.setLong(1, size);
			pstmt.setString(2, jobId);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 
		return false;
	}

	@Override
	public void deleteObjects(String jobId, String path, String versionId) {
		open();
		String sql;
		if (versionId == null || versionId.isEmpty()) {
			sql = SQL_DELETE_JOB + jobId + SQL_OBJECT_WHERE_PATH_WITH_VERSIONID_NULL;
		} else {
			sql = SQL_DELETE_JOB + jobId + SQL_OBJECT_WHERE_PATH_WITH_VERSIONID;
		}

		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(2, versionId);
			} 
			if (pstmt.executeUpdate() == 1) {
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}
	
	@Override
	public void updateJobEnd(String jobId) {
		open();
		try (Statement stmt = con.createStatement()) {
			stmt.executeUpdate(SQL_UPDATE_JOB_END + jobId);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public List<HashMap<String, Object>> status() {
		Connection con = null;
		con = getReadConnection();

		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> info = null;

		if (con == null) {
			return list;	
		}

		try (Statement stmt = con.createStatement();
			 ResultSet rs = stmt.executeQuery(SQL_SELECT_JOB_STATUS);) {
			while (rs.next()) {
				info = new HashMap<String, Object>();
				
				info.put(JOB_TABLE_COLUMN_JOB_ID, rs.getInt(1));
				info.put(JOB_TABLE_COLUMN_JOB_STATE, rs.getInt(2));
				info.put(JOB_TABLE_COLUMN_JOB_TYPE, rs.getString(3));
				info.put(JOB_TABLE_COLUMN_SOURCE_POINT, rs.getString(4));
				info.put(JOB_TABLE_COLUMN_TARGET_POINT, rs.getString(5));
				info.put(JOB_TABLE_COLUMN_OBJECTS_COUNT, rs.getLong(6));
				info.put(JOB_TABLE_COLUMN_OBJECTS_SIZE, rs.getLong(7));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT, rs.getLong(8));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE, rs.getLong(9));
				info.put(JOB_TABLE_COLUMN_FAILED_COUNT, rs.getLong(10));
				info.put(JOB_TABLE_COLUMN_FAILED_SIZE, rs.getLong(11));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT, rs.getLong(12));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE, rs.getLong(13));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT, rs.getLong(14));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE, rs.getLong(15));
				info.put(JOB_TABLE_COLUMN_START, rs.getString(16));
				info.put(JOB_TABLE_COLUMN_END, rs.getString(17));
				info.put(JOB_TABLE_COLUMN_ERROR_DESC, rs.getString(18));

				list.add(info);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		
		return list;
	}

	@Override
	public List<HashMap<String, Object>> status(String jobId) {
		Connection con = null;
		con = getReadConnection();

		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> info = null;

		if (con == null) {
			return list;	
		}

		try (PreparedStatement pstmt = con.prepareStatement(SQL_SELECT_JOB_STATUS_JOBID)) {
			pstmt.setString(1, jobId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				info = new HashMap<String, Object>();
				
				info.put(JOB_TABLE_COLUMN_JOB_ID, rs.getInt(1));
				info.put(JOB_TABLE_COLUMN_JOB_STATE, rs.getInt(2));
				info.put(JOB_TABLE_COLUMN_JOB_TYPE, rs.getString(3));
				info.put(JOB_TABLE_COLUMN_SOURCE_POINT, rs.getString(4));
				info.put(JOB_TABLE_COLUMN_TARGET_POINT, rs.getString(5));
				info.put(JOB_TABLE_COLUMN_OBJECTS_COUNT, rs.getLong(6));
				info.put(JOB_TABLE_COLUMN_OBJECTS_SIZE, rs.getLong(7));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT, rs.getLong(8));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE, rs.getLong(9));
				info.put(JOB_TABLE_COLUMN_FAILED_COUNT, rs.getLong(10));
				info.put(JOB_TABLE_COLUMN_FAILED_SIZE, rs.getLong(11));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT, rs.getLong(12));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE, rs.getLong(13));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT, rs.getLong(14));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE, rs.getLong(15));
				info.put(JOB_TABLE_COLUMN_START, rs.getString(16));
				info.put(JOB_TABLE_COLUMN_END, rs.getString(17));
				info.put(JOB_TABLE_COLUMN_ERROR_DESC, rs.getString(18));

				list.add(info);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		
		return list;
	}

	@Override
	public List<HashMap<String, Object>> status(String srcBucketName, String destBucketName) {
		Connection con = null;
		con = getReadConnection();

		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> info = null;

		if (con == null) {
			return list;	
		}

		try (PreparedStatement pstmt = con.prepareStatement(SQL_SELECT_JOB_STATUS_JOBID)) {
			pstmt.setString(1, "%" + srcBucketName + "%");
			pstmt.setString(1, "%" + destBucketName + "%");
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				info = new HashMap<String, Object>();
				
				info.put(JOB_TABLE_COLUMN_JOB_ID, rs.getInt(1));
				info.put(JOB_TABLE_COLUMN_JOB_STATE, rs.getInt(2));
				info.put(JOB_TABLE_COLUMN_JOB_TYPE, rs.getString(3));
				info.put(JOB_TABLE_COLUMN_SOURCE_POINT, rs.getString(4));
				info.put(JOB_TABLE_COLUMN_TARGET_POINT, rs.getString(5));
				info.put(JOB_TABLE_COLUMN_OBJECTS_COUNT, rs.getLong(6));
				info.put(JOB_TABLE_COLUMN_OBJECTS_SIZE, rs.getLong(7));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT, rs.getLong(8));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE, rs.getLong(9));
				info.put(JOB_TABLE_COLUMN_FAILED_COUNT, rs.getLong(10));
				info.put(JOB_TABLE_COLUMN_FAILED_SIZE, rs.getLong(11));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT, rs.getLong(12));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE, rs.getLong(13));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT, rs.getLong(14));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE, rs.getLong(15));
				info.put(JOB_TABLE_COLUMN_START, rs.getString(16));
				info.put(JOB_TABLE_COLUMN_END, rs.getString(17));
				info.put(JOB_TABLE_COLUMN_ERROR_DESC, rs.getString(18));

				list.add(info);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		
		return list;
	}

	@Override
	public List<HashMap<String, Object>> statusSrcBucket(String bucket) {
		Connection con = null;
		con = getReadConnection();

		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> info = null;

		if (con == null) {
			return list;	
		}

		try (PreparedStatement pstmt = con.prepareStatement(SQL_SELECT_JOB_STATUS_SRC_BUCKET)) {
			pstmt.setString(1, "%" + bucket + "%");
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				info = new HashMap<String, Object>();
				
				info.put(JOB_TABLE_COLUMN_JOB_ID, rs.getInt(1));
				info.put(JOB_TABLE_COLUMN_JOB_STATE, rs.getInt(2));
				info.put(JOB_TABLE_COLUMN_JOB_TYPE, rs.getString(3));
				info.put(JOB_TABLE_COLUMN_SOURCE_POINT, rs.getString(4));
				info.put(JOB_TABLE_COLUMN_TARGET_POINT, rs.getString(5));
				info.put(JOB_TABLE_COLUMN_OBJECTS_COUNT, rs.getLong(6));
				info.put(JOB_TABLE_COLUMN_OBJECTS_SIZE, rs.getLong(7));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT, rs.getLong(8));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE, rs.getLong(9));
				info.put(JOB_TABLE_COLUMN_FAILED_COUNT, rs.getLong(10));
				info.put(JOB_TABLE_COLUMN_FAILED_SIZE, rs.getLong(11));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT, rs.getLong(12));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE, rs.getLong(13));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT, rs.getLong(14));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE, rs.getLong(15));
				info.put(JOB_TABLE_COLUMN_START, rs.getString(16));
				info.put(JOB_TABLE_COLUMN_END, rs.getString(17));
				info.put(JOB_TABLE_COLUMN_ERROR_DESC, rs.getString(18));

				list.add(info);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		
		return list;
	}

	@Override
	public List<HashMap<String, Object>> statusDstBucket(String bucket) {
		Connection con = null;
		con = getReadConnection();

		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> info = null;

		if (con == null) {
			return list;	
		}

		try (PreparedStatement pstmt = con.prepareStatement(SQL_SELECT_JOB_STATUS_DST_BUCKET)) {
			pstmt.setString(1, "%" + bucket + "%");
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				info = new HashMap<String, Object>();
				
				info.put(JOB_TABLE_COLUMN_JOB_ID, rs.getInt(1));
				info.put(JOB_TABLE_COLUMN_JOB_STATE, rs.getInt(2));
				info.put(JOB_TABLE_COLUMN_JOB_TYPE, rs.getString(3));
				info.put(JOB_TABLE_COLUMN_SOURCE_POINT, rs.getString(4));
				info.put(JOB_TABLE_COLUMN_TARGET_POINT, rs.getString(5));
				info.put(JOB_TABLE_COLUMN_OBJECTS_COUNT, rs.getLong(6));
				info.put(JOB_TABLE_COLUMN_OBJECTS_SIZE, rs.getLong(7));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT, rs.getLong(8));
				info.put(JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE, rs.getLong(9));
				info.put(JOB_TABLE_COLUMN_FAILED_COUNT, rs.getLong(10));
				info.put(JOB_TABLE_COLUMN_FAILED_SIZE, rs.getLong(11));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT, rs.getLong(12));
				info.put(JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE, rs.getLong(13));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT, rs.getLong(14));
				info.put(JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE, rs.getLong(15));
				info.put(JOB_TABLE_COLUMN_START, rs.getString(16));
				info.put(JOB_TABLE_COLUMN_END, rs.getString(17));
				info.put(JOB_TABLE_COLUMN_ERROR_DESC, rs.getString(18));

				list.add(info);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		
		return list;
	}

	@Override
	public String getProcessId(String jobId) {
		String pid = null;
		Connection con = getReadConnection();
		if (con == null) {
			return null;
		}

		String sql = SQL_GET_PID + jobId;
		try (Statement stmt = con.createStatement();
			 ResultSet rs = stmt.executeQuery(sql);) {
			if (rs.next()) {
				pid = rs.getString(1);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		
		return pid;
	}

	@Override
	public void dropMoveObjectIndex(String jobId) {
		open();
		String sql = SQL_DROP_MOVE_OBJECT_INDEX + jobId + UNDER_INDEX;
		try(Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public void dropMoveObjectTable(String jobId) {
		open();
		String sql = SQL_DROP_MOVE_OBJECT + jobId + UNDER_OBJECTS;
		try(Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public String getJobType(String jobId) {
		String jobType = null;
		open();
		String sql = SQL_GET_JOB_TYPE + jobId;
		try (Statement stmt = con.createStatement();
			 ResultSet rs = stmt.executeQuery(sql);) {
			if (rs.next()) {
				jobType = rs.getString(1);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
		return jobType;
	}

	@Override
	public void updateJobRerun(String jobId) {
		open();
		String sql = SQL_INIT_JOB_RERUN + jobId;
		try (Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public void updateObjectsRerun(String jobId) {
		open();
		String sql = UPDATE_JOB_ID + jobId + SQL_INIT_MOVE_OBJECT_RERUN;
		try (Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public void updateJobStart(String jobId) {
		open();
		try (Statement stmt = con.createStatement()) {
			stmt.executeUpdate(SQL_UPDATE_JOB_START + jobId);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public void setProcessId(String jobId, String pid) {
		open();
		String sql = SQL_SET_PID + pid + WHERE_JOB_ID + jobId;
		try (Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public int stateWhenExistObject(String jobId, String path) {
		open();
		int state = -1;
		String sql = SQL_GET_OBJECT_STATE + jobId + SQL_OBJECT_WHERE_PATH;
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				state = rs.getInt(1);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		// String sql = SQL_GET_OBJECT_STATE + jobId + SQL_OBJECT_WHERE_PATH + path + SINGLE_QUOTATION;
		// try (Statement stmt = con.createStatement();
		// 	 ResultSet rs = stmt.executeQuery(sql);) {
		// 	if (rs.next()) {
		// 		state = rs.getInt(1);
		// 	} 
		// } catch (SQLException e) {
		// 	logger.error(e.getMessage());
		// } 
		return state;
	}

	@Override
	public String getMtime(String jobId, String path) {
		open();
		String mtime = null;
		String sql = SQL_GET_MTIME + jobId + SQL_OBJECT_WHERE_PATH;
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				mtime = rs.getString(1);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		// String sql = SQL_GET_MTIME + jobId + SQL_OBJECT_WHERE_PATH + path + SINGLE_QUOTATION;
		// try (Statement stmt = con.createStatement();
		// 	 ResultSet rs = stmt.executeQuery(sql);) {
		// 	if (rs.next()) {
		// 		mtime = rs.getString(1);
		// 	} 
		// } catch (SQLException e) {
		// 	logger.error(e.getMessage());
		// } 
		return mtime;
	}

	@Override
	public void updateDeleteMarker(String jobId, String path, String versionId) {
		open();
		String sql;
		if (versionId == null || versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_COMPLETE;
		} else {
			sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_VERSIONID_COMPLETE;
		} 
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(2, versionId);
			}
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		// String sql = UPDATE_JOB_ID + jobId + "_OBJECTS SET object_state = 3 WHERE path ='" + path + "' and version_id = '" + versionId + SINGLE_QUOTATION;
		// try (Statement stmt = con.createStatement()) {
		// 	stmt.executeUpdate(sql);
		// } catch (SQLException e) {
		// 	logger.error(e.getMessage());
		// } 
	}

	public static void createMoveObjectTableIndex(String jobId) {
		open();
		String sql = "CREATE INDEX PATH_" + jobId + "_INDEX ON JOB_" + jobId + "_OBJECTS (path)";
		try (Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}
	
	public static void updateObjectInfo(String jobId, String path, String mTime, long size) {
		open();
		String sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_INFO;
		// String sql = UPDATE_JOB_ID + jobId + SQL_SET_MOVE_OBJECT_INFO + path + SINGLE_QUOTATION;
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, mTime);
			pstmt.setLong(2, size);
			pstmt.setString(3, path);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public static synchronized boolean updateJobMovedCount(String jobId, long count, long size) {
		open();
		try (PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_MOVED_COUNT)) {
			pstmt.setLong(1, count);
			pstmt.setLong(2, size);
			pstmt.setString(3, jobId);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		}
		return false;
	}

	@Override
	public void createTargetObjectTable(String jobId) {
		open();
		String sql = "CREATE TABLE IF NOT EXISTS JOB_" + jobId + "_TARGET_OBJECTS (\n"
				+ "'path' TEXT NOT NULL,\n"
				+ "'version_id' TEXT,\n"
				+ "'size' INTEGER NOT NULL,\n"
				+ "'etag' TEXT,\n"
				+ "UNIQUE('path', 'version_id'))";
		try(Statement stmt = con.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} 
	}

	@Override
	public boolean compareObject(String jobId, String path, long size, String etag) {
		open();
		String sql = null;
		if (etag.lastIndexOf("-") != -1) {
			sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_PATH_WITH_SIZE;
		} else {
			sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_PATH_WITH_ETAG;
		}

		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (etag.lastIndexOf("-") != -1) {
				pstmt.setLong(2, size);
			} else {
				pstmt.setString(2, etag);
			}

			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 
		// try (Statement stmt = con.createStatement();
		// 	ResultSet rs = stmt.executeQuery(sql);) {
	   	// 	while (rs.next()) {
		// 		return true;
		// 	}
	   	// } catch (SQLException e) {
	   	// 	logger.error(e.getMessage());
   		// } 
   
   		return false;
	}

	@Override
	public boolean insertTargetObject(String jobId, String path, String versionId, long size, String etag) {
		open();
		String sql = SQL_REPLACE + jobId + SQL_INSERT_TARGET_OBJECT;
		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			pstmt.setString(2, versionId);
			pstmt.setLong(3, size);
			pstmt.setString(4, etag);

			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean updateSkipObject(String jobId, String path, String versionId) {
		open();
		String sql = null;

		if (versionId == null || versionId.isEmpty()) {
			sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK_WHERE_PATH_WITH_VERSION_NULL;
		} else {
			sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK_WHERE_PATH_WITH_VERSION;
		}

		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			if (versionId != null && !versionId.isEmpty()) {
				pstmt.setString(2, versionId);
			}

			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 
		// if (versionId == null || versionId.isEmpty()) {
		// 	sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK_WHERE_PATH + path + SQL_VERSIONID_IS_NULL;
		// } else {
		// 	sql = UPDATE_JOB_ID + jobId + SQL_SKIP_CHECK_WHERE_PATH + path + SQL_VERSIONID + versionId + SINGLE_QUOTATION;
		// }

		// try(Statement stmt = con.createStatement()) {
		// 	if (stmt.executeUpdate(sql) == 1) {
		// 		return true;
		// 	}
		// } catch (SQLException e) {
		// 	logger.warn(e.getMessage());
		// } 

		return false;
	}

	@Override
	public boolean updateJobSkipInfo(String jobId, long size) {
		open();
		try(PreparedStatement pstmt = con.prepareStatement(SQL_UPDATE_JOB_SKIP)) {
			pstmt.setLong(1, size);
			pstmt.setString(2, jobId);
			if (pstmt.executeUpdate() == 1) {
				return true;
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		} 

		return false;
	}

	@Override
	public boolean compareObject(String jobId, String path, String etag) {
		open();
		String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_PATH_WITH_ETAG;
		// String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_PATH + path + SQL_GET_TARGET_OBJECT_ETAG + etag + SINGLE_QUOTATION;

		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			pstmt.setString(2, etag);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		// try (Statement stmt = con.createStatement();
		// 	ResultSet rs = stmt.executeQuery(sql);) {
	   	// 	while (rs.next()) {
		// 		return true;
		// 	}
	   	// } catch (SQLException e) {
	   	// 	logger.error(e.getMessage());
   		// } 
   
   		return false;
	}

	@Override
	public boolean compareObject(String jobId, String path, long size) {
		open();
		String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_PATH_WITH_SIZE;
		// String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_PATH + path + SQL_GET_TARGET_OBJECT_SIZE + size + SINGLE_QUOTATION;

		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			pstmt.setLong(2, size);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		// try (Statement stmt = con.createStatement();
		// 	ResultSet rs = stmt.executeQuery(sql);) {
	   	// 	while (rs.next()) {
		// 		return true;
		// 	}
	   	// } catch (SQLException e) {
	   	// 	logger.error(e.getMessage());
   		// } 
   
   		return false;
	}

	@Override
	public boolean isExistObject(String jobId, String path) {
		open();
		String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_PATH;
		// String sql = SQL_GET_TARGET + jobId + SQL_GET_TARGET_OBJECT_PATH + path + SINGLE_QUOTATION;

		try(PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setString(1, path);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		// try (Statement stmt = con.createStatement();
		// 	ResultSet rs = stmt.executeQuery(sql);) {
	   	// 	while (rs.next()) {
		// 		return true;
		// 	}
	   	// } catch (SQLException e) {
	   	// 	logger.error(e.getMessage());
   		// } 
   
   		return false;
	}

	@Override
	public long insertMoveObject(String jobId, ObjectListing objectListing) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'insertMoveObject'");
	}

	@Override
	public long insertMoveObjectVersioning(String jobId, VersionListing versionListing) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'insertMoveObjectVersioning'");
	}

	@Override
	public boolean updateJobInfo(String jobId, int count, long size) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateJobInfo'");
	}

	@Override
	public boolean updateJobRerunInfo(String jobId, int count, long size) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateJobRerunInfo'");
	}

	@Override
	public void dropRerunObjectIndex(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'dropRerunObjectIndex'");
	}

	@Override
	public void dropRerunObjectTable(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'dropRerunObjectTable'");
	}

	@Override
	public void createRerunObjectTable(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'createRerunObjectTable'");
	}

	@Override
	public long insertRerunObject(String jobId, ObjectListing objectListing) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'insertRerunObject'");
	}

	@Override
	public long insertRerunObjectVersioning(String jobId, VersionListing versionListing) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'insertRerunObjectVersion'");
	}

	@Override
	public void renameTable(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'renameTable'");
	}

	@Override
	public long getMaxSequenceRerun(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'getMaxSequenceRerun'");
	}

	@Override
	public List<MoveData> getToRerunObjectsInfo(String jobId, long sequence, long limit) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'getToRerunObjectsInfo'");
	}

	@Override
	public List<HashMap<String, Object>> getToRerunDeleteObjectsInfo(String jobId, long sequence, long limit) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'getToRerunDeleteObjectsInfo'");
	}

	@Override
	public void updateSkipRerun(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateSkipRerun'");
	}

	@Override
	public boolean updateJobRerunSkipInfo(String jobId, long count, long size) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateJobRerunSkipInfo'");
	}

	@Override
	public void infoSkipRerun(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'infoSkipRerun'");
	}

	@Override
	public boolean isExistMoveTable(String db, String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'isExistMoveTable'");
	}

	@Override
	public boolean isExistRerunTable(String db, String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'isExistRerunTable'");
	}

	@Override
	public boolean updateObjectRerunComplete(String jobId, String path, String versionId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateObjectRerunComplete'");
	}

	@Override
	public boolean updateObjectRerunEventFailed(String jobId, String path, String versionId, String errorCode,
			String errorMessage) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateObjectRerunEventFailed'");
	}

	@Override
	public boolean updateObjectRerun(String jobId, String path, String versionId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateObjectRerun'");
	}

	@Override
	public void updateRerunDeleteMarker(String jobId, String path, String versionId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateRerunDeleteMarker'");
	}

	@Override
	public void checkDeleteObjectsForReRun(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'checkDeleteObjectsForReRun'");
	}

	@Override
	public boolean updateJobMoved(String jobId, int count, long size) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateJobMoved'");
	}

	@Override
	public boolean updateJobFailedInfo(String jobId, int count, long size) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateJobFailedInfo'");
	}

	@Override
	public boolean updateObjectMoveComplete(String jobId, List<HashMap<String, Object>> list) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateObjectMoveComplete'");
	}

	@Override
	public boolean updateObjectRerunComplete(String jobId, List<HashMap<String, Object>> list) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateObjectRerunComplete'");
	}

	@Override
	public boolean updateObjectMoveEventFailed(String jobId, List<HashMap<String, Object>> list) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateObjectMoveEventFailed'");
	}

	@Override
	public boolean updateObjectRerunEventFailed(String jobId, List<HashMap<String, Object>> list) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateObjectRerunEventFailed'");
	}

	@Override
	public List<HashMap<String, Object>> getToMoveObjectsInfoVersioning(String jobId, long sequence, long limit) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'getToMoveObjectsInfoVersioning'");
	}

	@Override
	public boolean updateJobResult(String jobId, boolean result, String path, String versionId, long size,
			boolean isRerun) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'updateJobResult'");
	}

	@Override
	public boolean deleteRerunTableForDeletedObjects(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'deleteRerunTableForDeletedObjects'");
	}

	@Override
	public int getJobState(String jobId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'getJobStatus'");
	}

	@Override
	public long insertMoveObject(String jobId, List<MoveData> list) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'insertMoveObject'");
	}

}
