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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import com.google.common.base.Strings;

import ifs_mover.db.MariaDB;
import ifs_mover.repository.Repository;

public class Main {
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	private static final String JOBID_PATH = "./.jobId";
	private static final int THREAD_COUNT = 5;
	
	private static final long UNIT_G = (1024 * 1024 * 1024);
	private static final long UNIT_M = (1024 * 1024);
	private static final long UNIT_K = 1024;
	
	private static final int STATE_INIT = 0;
	private static final int STATE_MOVE = 1;
	private static final int STATE_COMPLETE = 4;
	private static final int STATE_STOP = 5;
	private static final int STATE_REMOVE = 6;
	private static final int STATE_RERUN = 7;
	private static final int STATE_RERUN_MOVE = 8;
	private static final int STATE_ERROR = 10;
	
	private static final String IFS_MOVER_END = "IFS_MOVER({}) END";

	private static final String JOB_IS = "Job : ";
	private static final String PREPARE = "Preparing";
	private static final String PROGRESS = "Progress";
	private static final String TOTAL = "Total";
	private static final String MOVED = "Moved";
	private static final String FAILED = "Failed";
	private static final String SKIPPED = "Skipped";
	private static final String DELETED = "Deleted";
	private static final String FORMAT_START = "%-5s\t%-15s%22s";
	private static final String FORMAT_START_END = "%-5s\t%-15s%22s - %s";
	private static final String FORMAT_G = "%-10s : %,14d/ %,10.2fG";
	private static final String FORMAT_M = "%-10s : %,14d/ %,10.2fM";
	private static final String FORMAT_K = "%-10s : %,14d/ %,10.2fK";
	private static final String FORMAT_B = "%-10s : %,14d/ %,10dB";

	public static void main(String[] args) {
		
		IMOptions options = new IMOptions(args);
		options.handleOptions();
		
		IMOptions.WORK_TYPE type = options.getWorkType();
		
		ManagementFactory.getRuntimeMXBean();
		RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
		String processID = rt.getName();
		  
		String pid = processID.substring(0, processID.indexOf("@"));
		
		String jobId = null;
		int threadCount = options.getThreadCount();
		if (threadCount <= 0) {
			threadCount = THREAD_COUNT;
		}
		
		// DBManager.init();
		// String directoryName = System.getProperty("user.dir");
		// System.out.println("Current Working Directory is = " + directoryName);

		MoverConfig config = MoverConfig.getInstance();
		config.configure();
		logger.info("db init ...");
		try {
			Utils.getDBInstance().init(config.getDbHost(), config.getDbPort(), config.getDatabase(), config.getDbUser(), config.getDbPass(), config.getDbPoolSize());
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(-1);
		}

		// disabling the integrity verification
		// System.setProperty(SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY,"true");
		
		switch (type) {
		case CHECK:
			logger.info("IFS_MOVER({}) CHECK START", pid);
			ObjectMover checkObjectMover = new ObjectMover.Builder(jobId)
				.sourceConfig(options.getSourceConfig())
				.targetConfig(options.getTargetConfig())
				.threadCount(threadCount)
				.type(options.getType())
				.isRerun(false)
				.build();
			
			checkObjectMover.check();
			System.out.println("Check success.");
			logger.info(IFS_MOVER_END, pid);
			break;
			
		case MOVE:
			Utils.getDBInstance().createJob(pid, options.getType(), options.getSourceConfig(), options.getTargetConfig());
			jobId = Utils.getDBInstance().getJobId(pid);

			writeJobID(jobId, options.getSourceConfPath());
			logger.info("create jobid table ...");
			Utils.getDBInstance().createMoveObjectTable(jobId);
			Utils.getDBInstance().createTargetObjectTable(jobId);
			
			MDC.put("logFileName", "ifs_mover." + jobId + ".log");
			logger.info("IFS_MOVER({}) MOVE START", pid);
			ObjectMover objectMover = new ObjectMover.Builder(jobId)
					.sourceConfig(options.getSourceConfig())
					.targetConfig(options.getTargetConfig())
					.threadCount(threadCount)
					.type(options.getType())
					.isRerun(false)
					.build();
			objectMover.init();
			Utils.getDBInstance().updateJobState(jobId, type);
			objectMover.moveObjects();
			Utils.getDBInstance().updateJobState(jobId, IMOptions.WORK_TYPE.COMPLETE);
			Utils.getDBInstance().updateJobEnd(jobId);
			logger.info(IFS_MOVER_END, pid);
			break;
			
		case STOP:
			jobId = options.getStopId();
			logger.info("IFS_MOVER({}) STOP", pid);
			// String pidOfJob = DBManager.getProcessId(jobId);
			String pidOfJob = Utils.getDBInstance().getProcessId(jobId);
			if (pidOfJob == null) {
				System.out.println("Can't find pid with jobID : " + jobId);
				logger.error("Can't find pid with jobID : {}", jobId);
				logger.info(IFS_MOVER_END, pid);
				return;
			}
			
			try {
				Process p = Runtime.getRuntime().exec("kill -9 " + pidOfJob);
				p.waitFor();
				// DBManager.updateJobState(jobId, type);
				Utils.getDBInstance().updateJobState(jobId, type);
			} catch (InterruptedException | IOException e) {
				logger.error("faild stop job : {} - {}", jobId, e.getMessage());
			}
			// DBManager.updateJobEnd(jobId);
			Utils.getDBInstance().updateJobEnd(jobId);
			logger.info(IFS_MOVER_END, pid);
			break;
			
		case REMOVE:
			logger.info("IFS_MOVER({}) REMOVE", pid);
			jobId = options.getRemoveId();
			// DBManager.dropMoveObjectIndex(jobId);
			// DBManager.dropMoveObjectTable(jobId);
			// DBManager.updateJobState(jobId, type);
			Utils.getDBInstance().dropMoveObjectIndex(jobId);
			Utils.getDBInstance().dropMoveObjectTable(jobId);
			Utils.getDBInstance().updateJobState(jobId, type);
			logger.info(IFS_MOVER_END, pid);
			break;
			
		case RERUN:
			jobId = options.getRerunId();

			MDC.put("logFileName", "ifs_mover." + jobId + ".log");
			logger.info("IFS_MOVER({}) RERUN START", pid);
			String job_type = Utils.getDBInstance().getJobType(jobId);
			if (job_type == null) {
				logger.error("check job_id({}}) : There is no job.", jobId);
				logger.info("IFS_MOVER({}) RERUN END", pid);
				System.exit(-1);
			}

			Utils.getDBInstance().updateJobState(jobId, IMOptions.WORK_TYPE.RERUN);
			
			String pidOfRunJob = Utils.getDBInstance().getProcessId(jobId);
			
			if (pidOfRunJob != null) {
				try {
					Process p = Runtime.getRuntime().exec("kill -9 " + pidOfRunJob);
					p.waitFor();
				} catch (InterruptedException | IOException e) {
					logger.error("faild stop job : {} - {}", jobId, e.getMessage());
				}
			}

			ObjectMover rerunObjectMover = new ObjectMover.Builder(jobId)
					.sourceConfig(options.getSourceConfig())
					.targetConfig(options.getTargetConfig())
					.threadCount(threadCount)
					.type(job_type)
					.isRerun(true)
					.build();

			Utils.getDBInstance().updateJobStart(jobId);
			Utils.getDBInstance().setProcessId(jobId, pid);
			Utils.getDBInstance().updateJobRerun(jobId);
			Utils.getDBInstance().updateObjectsRerun(jobId);

			rerunObjectMover.init();
			Utils.getDBInstance().updateJobState(jobId, IMOptions.WORK_TYPE.RERUN_MOVE);
			rerunObjectMover.moveObjects();
			Utils.getDBInstance().updateJobState(jobId, IMOptions.WORK_TYPE.COMPLETE);
			Utils.getDBInstance().updateJobEnd(jobId);
			logger.info("IFS_MOVER({}) RERUN END", pid);
			break;
			
		case STATUS:
			status(options.getJobId(), options.getSrcBucketName(), options.getDstBucketName());
			
			logger.info("IFS_MOVER({}) STATUS", pid);
			break;

		default:
			System.exit(-1);
		}
	}

	private static void status(String markedJobId, String srcBucketName, String dstBucketName) {
		String jobId;
		int jobState;
		String jobType;
		String sourcePoint;
		String targetPoint;
		long objectsCount;
		long objectsSize;
		long movedObjectsCount;
		long movedObjectsSize;
		long failedCount;
		long failedSize;
		long skipObjectsCount;
		long skipObjectsSize;
		long deleteObjectCount;
		long deleteObjectSize;
		String startTime;
		String endTime;
		String errorDesc;
		
		double unitSize = 0.0;
		double unitMove = 0.0;
		double unitFailed = 0.0;
		double unitSkip = 0.0;
		double unitDelete = 0.0;
		double percent = 0.0;
	
		List<HashMap<String, Object>> list;
		if (!Strings.isNullOrEmpty(markedJobId)) {
			list = Utils.getDBInstance().status(markedJobId);
		} else if (!Strings.isNullOrEmpty(srcBucketName) && !Strings.isNullOrEmpty(dstBucketName)) {
			list = Utils.getDBInstance().status(srcBucketName, dstBucketName);
		} else if (!Strings.isNullOrEmpty(srcBucketName)) {
			list = Utils.getDBInstance().statusSrcBucket(srcBucketName);
		} else if (!Strings.isNullOrEmpty(dstBucketName)) {
			list = Utils.getDBInstance().statusDstBucket(dstBucketName);
		} else {
			list = Utils.getDBInstance().status();
		}
	
		if (list == null) {
			System.out.println("No jobs were created.");
			return;
		}
	
		for (HashMap<String, Object> info : list) {
			jobId = String.valueOf((int) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_ID));
			jobState = (int) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_STATE);
			jobType = (String) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_TYPE);
			sourcePoint = (String) info.get(MariaDB.JOB_TABLE_COLUMN_SOURCE_POINT);
			targetPoint = (String) info.get(MariaDB.JOB_TABLE_COLUMN_TARGET_POINT);
			objectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_OBJECTS_COUNT);
			objectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_OBJECTS_SIZE);
			movedObjectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT);
			movedObjectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE);
			failedCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_FAILED_COUNT);
			failedSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_FAILED_SIZE);
			skipObjectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT);
			skipObjectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE);
			deleteObjectCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT);
			deleteObjectSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE);
			startTime = (String) info.get(MariaDB.JOB_TABLE_COLUMN_START);
			endTime = (String) info.get(MariaDB.JOB_TABLE_COLUMN_END);
			errorDesc = (String) info.get(MariaDB.JOB_TABLE_COLUMN_ERROR_DESC);
	
			if (jobState == STATE_REMOVE) {
				continue;
			}
	
			switch (jobState) {
			case STATE_INIT:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "INIT...", startTime));
				break;
				
			case STATE_MOVE:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "MOVE...", startTime));
				break;
				
			case STATE_COMPLETE:
				System.out.println(JOB_IS + String.format(FORMAT_START_END, jobId, "COMPLETED", startTime, endTime));
				break;
				
			case STATE_STOP:
				System.out.println(JOB_IS + String.format(FORMAT_START_END, jobId, "STOPPED", startTime, endTime));
				break;
				
			case STATE_RERUN:
				System.out.println(JOB_IS + String.format("%-5s\t%-15s%s", jobId, "RERUN INIT...", startTime));
				break;
				
			case STATE_RERUN_MOVE:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "RERUN MOVE", startTime));
				break;
				
			case STATE_ERROR:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "ERROR", startTime));
				break;
				
			default:
				break;
			}
			
			if (Repository.IFS_FILE.compareToIgnoreCase(jobType) == 0) {
				System.out.println("File : " + String.format("%s -> Object : %s", sourcePoint, targetPoint));
			} else {
				System.out.println("Object : " + String.format("%s -> Object : %s", sourcePoint, targetPoint));
			}
			
			if (jobState == STATE_ERROR) {
				System.out.println("Error : " + errorDesc);
				System.out.println();
				continue;
			}
	
			if (jobState == STATE_INIT) {
				unitSize = (double)objectsSize / UNIT_G;
				if (unitSize > 1.0) {
					System.out.println(String.format(FORMAT_G, PREPARE, objectsCount, unitSize));
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						System.out.println(String.format(FORMAT_M, PREPARE, objectsCount, unitSize));
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							System.out.println(String.format(FORMAT_K, PREPARE, objectsCount, unitSize));
						} else {
							System.out.println(String.format(FORMAT_B, PREPARE, objectsCount, objectsSize));
						}
					}
				}
			} else if (jobState == STATE_RERUN) {
				unitSize = (double)objectsSize / UNIT_G;
				if (unitSize > 1.0) {
					System.out.println(String.format(FORMAT_G, PREPARE, objectsCount, unitSize));
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						System.out.println(String.format(FORMAT_M, PREPARE, objectsCount, unitSize));
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							System.out.println(String.format(FORMAT_K, PREPARE, objectsCount, unitSize));
						} else {
							System.out.println(String.format(FORMAT_B, PREPARE, objectsCount, objectsSize));
						}
					}
				}
				
				if (skipObjectsCount > 0) {
					unitSkip = (double)skipObjectsSize / UNIT_G;
					if (unitSkip > 1.0) {
						System.out.println(String.format(FORMAT_G, SKIPPED, skipObjectsCount, unitSkip));
					} else {
						unitSkip = (double)skipObjectsSize / UNIT_M;
						if (unitSkip > 1.0) {
							System.out.println(String.format(FORMAT_M, SKIPPED, skipObjectsCount, unitSkip));
						} else {
							unitSkip = (double)skipObjectsSize / UNIT_K;
							if (unitSkip > 1.0) {
								System.out.println(String.format(FORMAT_K, SKIPPED, skipObjectsCount, unitSkip));
							} else {
								System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
				}
				
				if (failedCount > 0) {
					unitFailed = (double)failedSize / UNIT_G;
					if (unitFailed > 1.0) {
						System.out.println(String.format(FORMAT_G, FAILED, failedCount, unitFailed));
					} else {
						unitFailed = (double)failedSize / UNIT_M;
						if (unitFailed > 1.0) {
							System.out.println(String.format(FORMAT_M, FAILED, failedCount, unitFailed));
						} else {
							unitFailed = (double)failedSize / UNIT_K;
							if (unitFailed > 1.0) {
								System.out.println(String.format(FORMAT_K, FAILED, failedCount, unitFailed));
							} else {
								System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
				}
			} else {
				if (objectsCount == 0) {
					percent = 0.0;
				} else {
					percent = (((double)skipObjectsSize + (double)movedObjectsSize + (double)failedSize) / (double) objectsSize) * 100;
				}
				
				unitSize = (double)objectsSize / UNIT_G;
				
				if (unitSize > 1.0) {
					unitMove = (double)movedObjectsSize / UNIT_G;
					if (unitMove > 1.0) {
						System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fG", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
					} else {
						unitMove = (double) movedObjectsSize / UNIT_M;
						if (unitMove > 1.0) {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fM", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
						} else {
							unitMove = (double) movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						}
					}
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						unitMove = (double)movedObjectsSize / UNIT_M;
						if (unitMove > 1.0) {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10.2fM", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
						} else {
							unitMove = (double)movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						}
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							unitMove = (double)movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fK\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fK\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						} else {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10dB\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, objectsSize, MOVED, movedObjectsCount, movedObjectsSize));
						}
					}
				}
				
				if (skipObjectsCount > 0) {
					unitSkip = (double)skipObjectsSize / UNIT_G;
					if (unitSkip > 1.0) {
						System.out.println(String.format(FORMAT_G, SKIPPED, skipObjectsCount, unitSkip));
					} else {
						unitSkip = (double)skipObjectsSize / UNIT_M;
						if (unitSkip > 1.0) {
							System.out.println(String.format(FORMAT_M, SKIPPED, skipObjectsCount, unitSkip));
						} else {
							unitSkip = (double)skipObjectsSize / UNIT_K;
							if (unitSkip > 1.0) {
								System.out.println(String.format(FORMAT_K, SKIPPED, skipObjectsCount, unitSkip));
							} else {
								System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
				}
				
				if (failedCount > 0) {
					unitFailed = (double)failedSize / UNIT_G;
					if (unitFailed > 1.0) {
						System.out.println(String.format(FORMAT_G, FAILED, failedCount, unitFailed));
					} else {
						unitFailed = (double)failedSize / UNIT_M;
						if (unitFailed > 1.0) {
							System.out.println(String.format(FORMAT_M, FAILED, failedCount, unitFailed));
						} else {
							unitFailed = (double)failedSize / UNIT_K;
							if (unitFailed > 1.0) {
								System.out.println(String.format(FORMAT_K, FAILED, failedCount, unitFailed));
							} else {
								System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
				}

				if (deleteObjectCount > 0) {
					unitDelete = (double)deleteObjectSize / UNIT_G;
					if (unitDelete > 1.0) {
						System.out.println(String.format(FORMAT_G, DELETED, deleteObjectCount, unitDelete));
					} else {
						unitDelete = (double)deleteObjectSize / UNIT_M;
						if (unitDelete > 1.0) {
							System.out.println(String.format(FORMAT_M, DELETED, deleteObjectCount, unitDelete));
						} else {
							unitDelete = (double)deleteObjectSize / UNIT_K;
							if (unitDelete > 1.0) {
								System.out.println(String.format(FORMAT_K, DELETED, deleteObjectCount, unitDelete));
							} else {
								System.out.println(String.format(FORMAT_B, DELETED, deleteObjectCount, deleteObjectSize));
							}
						}
					}
				}
			} 
			System.out.println();
		}
	}

	private static void statusSrcBucket(String bucket) {
		String jobId;
		int jobState;
		String jobType;
		String sourcePoint;
		String targetPoint;
		long objectsCount;
		long objectsSize;
		long movedObjectsCount;
		long movedObjectsSize;
		long failedCount;
		long failedSize;
		long skipObjectsCount;
		long skipObjectsSize;
		long deleteObjectCount;
		long deleteObjectSize;
		String startTime;
		String endTime;
		String errorDesc;
		
		double unitSize = 0.0;
		double unitMove = 0.0;
		double unitFailed = 0.0;
		double unitSkip = 0.0;
		double unitDelete = 0.0;
		double percent = 0.0;
	
		// List<Map<String, String>> list = DBManager.status();
		List<HashMap<String, Object>> list = Utils.getDBInstance().statusSrcBucket(bucket);
	
		if (list == null) {
			System.out.println("No jobs were created.");
			return;
		}
	
		for (HashMap<String, Object> info : list) {
			jobId = String.valueOf((int) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_ID));
			jobState = (int) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_STATE);
			jobType = (String) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_TYPE);
			sourcePoint = (String) info.get(MariaDB.JOB_TABLE_COLUMN_SOURCE_POINT);
			targetPoint = (String) info.get(MariaDB.JOB_TABLE_COLUMN_TARGET_POINT);
			objectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_OBJECTS_COUNT);
			objectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_OBJECTS_SIZE);
			movedObjectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT);
			movedObjectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE);
			failedCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_FAILED_COUNT);
			failedSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_FAILED_SIZE);
			skipObjectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT);
			skipObjectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE);
			deleteObjectCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT);
			deleteObjectSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE);
			startTime = (String) info.get(MariaDB.JOB_TABLE_COLUMN_START);
			endTime = (String) info.get(MariaDB.JOB_TABLE_COLUMN_END);
			errorDesc = (String) info.get(MariaDB.JOB_TABLE_COLUMN_ERROR_DESC);
	
			if (jobState == STATE_REMOVE) {
				continue;
			}
	
			switch (jobState) {
			case STATE_INIT:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "INIT...", startTime));
				break;
				
			case STATE_MOVE:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "MOVE...", startTime));
				break;
				
			case STATE_COMPLETE:
				System.out.println(JOB_IS + String.format(FORMAT_START_END, jobId, "COMPLETED", startTime, endTime));
				break;
				
			case STATE_STOP:
				System.out.println(JOB_IS + String.format(FORMAT_START_END, jobId, "STOPPED", startTime, endTime));
				break;
				
			case STATE_RERUN:
				System.out.println(JOB_IS + String.format("%-5s\t%-15s%s", jobId, "RERUN INIT...", startTime));
				break;
				
			case STATE_RERUN_MOVE:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "RERUN MOVE", startTime));
				break;
				
			case STATE_ERROR:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "ERROR", startTime));
				break;
				
			default:
				break;
			}
			
			if (Repository.IFS_FILE.compareToIgnoreCase(jobType) == 0) {
				System.out.println("File : " + String.format("%s -> Object : %s", sourcePoint, targetPoint));
			} else {
				System.out.println("Object : " + String.format("%s -> Object : %s", sourcePoint, targetPoint));
			}
			
			if (jobState == STATE_ERROR) {
				System.out.println("Error : " + errorDesc);
				System.out.println();
				continue;
			}
	
			if (jobState == STATE_INIT) {
				unitSize = (double)objectsSize / UNIT_G;
				if (unitSize > 1.0) {
					System.out.println(String.format(FORMAT_G, PREPARE, objectsCount, unitSize));
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						System.out.println(String.format(FORMAT_M, PREPARE, objectsCount, unitSize));
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							System.out.println(String.format(FORMAT_K, PREPARE, objectsCount, unitSize));
						} else {
							System.out.println(String.format(FORMAT_B, PREPARE, objectsCount, objectsSize));
						}
					}
				}
			} else if (jobState == STATE_RERUN) {
				unitSize = (double)objectsSize / UNIT_G;
				if (unitSize > 1.0) {
					System.out.println(String.format(FORMAT_G, PREPARE, objectsCount, unitSize));
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						System.out.println(String.format(FORMAT_M, PREPARE, objectsCount, unitSize));
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							System.out.println(String.format(FORMAT_K, PREPARE, objectsCount, unitSize));
						} else {
							System.out.println(String.format(FORMAT_B, PREPARE, objectsCount, objectsSize));
						}
					}
				}
				
				if (skipObjectsCount > 0) {
					unitSkip = (double)skipObjectsSize / UNIT_G;
					if (unitSkip > 1.0) {
						System.out.println(String.format(FORMAT_G, SKIPPED, skipObjectsCount, unitSkip));
					} else {
						unitSkip = (double)skipObjectsSize / UNIT_M;
						if (unitSkip > 1.0) {
							System.out.println(String.format(FORMAT_M, SKIPPED, skipObjectsCount, unitSkip));
						} else {
							unitSkip = (double)skipObjectsSize / UNIT_K;
							if (unitSkip > 1.0) {
								System.out.println(String.format(FORMAT_K, SKIPPED, skipObjectsCount, unitSkip));
							} else {
								System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
				}
				
				if (failedCount > 0) {
					unitFailed = (double)failedSize / UNIT_G;
					if (unitFailed > 1.0) {
						System.out.println(String.format(FORMAT_G, FAILED, failedCount, unitFailed));
					} else {
						unitFailed = (double)failedSize / UNIT_M;
						if (unitFailed > 1.0) {
							System.out.println(String.format(FORMAT_M, FAILED, failedCount, unitFailed));
						} else {
							unitFailed = (double)failedSize / UNIT_K;
							if (unitFailed > 1.0) {
								System.out.println(String.format(FORMAT_K, FAILED, failedCount, unitFailed));
							} else {
								System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
				}
			} else {
				if (objectsCount == 0) {
					percent = 0.0;
				} else {
					percent = (((double)skipObjectsSize + (double)movedObjectsSize + (double)failedSize) / (double) objectsSize) * 100;
				}
				
				unitSize = (double)objectsSize / UNIT_G;
				
				if (unitSize > 1.0) {
					unitMove = (double)movedObjectsSize / UNIT_G;
					if (unitMove > 1.0) {
						System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fG", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
					} else {
						unitMove = (double) movedObjectsSize / UNIT_M;
						if (unitMove > 1.0) {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fM", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
						} else {
							unitMove = (double) movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						}
					}
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						unitMove = (double)movedObjectsSize / UNIT_M;
						if (unitMove > 1.0) {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10.2fM", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
						} else {
							unitMove = (double)movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						}
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							unitMove = (double)movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fK\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fK\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						} else {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10dB\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, objectsSize, MOVED, movedObjectsCount, movedObjectsSize));
						}
					}
				}
				
				if (skipObjectsCount > 0) {
					unitSkip = (double)skipObjectsSize / UNIT_G;
					if (unitSkip > 1.0) {
						System.out.println(String.format(FORMAT_G, SKIPPED, skipObjectsCount, unitSkip));
					} else {
						unitSkip = (double)skipObjectsSize / UNIT_M;
						if (unitSkip > 1.0) {
							System.out.println(String.format(FORMAT_M, SKIPPED, skipObjectsCount, unitSkip));
						} else {
							unitSkip = (double)skipObjectsSize / UNIT_K;
							if (unitSkip > 1.0) {
								System.out.println(String.format(FORMAT_K, SKIPPED, skipObjectsCount, unitSkip));
							} else {
								System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
				}
				
				if (failedCount > 0) {
					unitFailed = (double)failedSize / UNIT_G;
					if (unitFailed > 1.0) {
						System.out.println(String.format(FORMAT_G, FAILED, failedCount, unitFailed));
					} else {
						unitFailed = (double)failedSize / UNIT_M;
						if (unitFailed > 1.0) {
							System.out.println(String.format(FORMAT_M, FAILED, failedCount, unitFailed));
						} else {
							unitFailed = (double)failedSize / UNIT_K;
							if (unitFailed > 1.0) {
								System.out.println(String.format(FORMAT_K, FAILED, failedCount, unitFailed));
							} else {
								System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
				}

				if (deleteObjectCount > 0) {
					unitDelete = (double)deleteObjectSize / UNIT_G;
					if (unitDelete > 1.0) {
						System.out.println(String.format(FORMAT_G, DELETED, deleteObjectCount, unitDelete));
					} else {
						unitDelete = (double)deleteObjectSize / UNIT_M;
						if (unitDelete > 1.0) {
							System.out.println(String.format(FORMAT_M, DELETED, deleteObjectCount, unitDelete));
						} else {
							unitDelete = (double)deleteObjectSize / UNIT_K;
							if (unitDelete > 1.0) {
								System.out.println(String.format(FORMAT_K, DELETED, deleteObjectCount, unitDelete));
							} else {
								System.out.println(String.format(FORMAT_B, DELETED, deleteObjectCount, deleteObjectSize));
							}
						}
					}
				}
			} 
			System.out.println();
		}
	}

	private static void statusDstBucket(String bucket) {
		String jobId;
		int jobState;
		String jobType;
		String sourcePoint;
		String targetPoint;
		long objectsCount;
		long objectsSize;
		long movedObjectsCount;
		long movedObjectsSize;
		long failedCount;
		long failedSize;
		long skipObjectsCount;
		long skipObjectsSize;
		long deleteObjectCount;
		long deleteObjectSize;
		String startTime;
		String endTime;
		String errorDesc;
		
		double unitSize = 0.0;
		double unitMove = 0.0;
		double unitFailed = 0.0;
		double unitSkip = 0.0;
		double unitDelete = 0.0;
		double percent = 0.0;
	
		// List<Map<String, String>> list = DBManager.status();
		List<HashMap<String, Object>> list = Utils.getDBInstance().statusDstBucket(bucket);
	
		if (list == null) {
			System.out.println("No jobs were created.");
			return;
		}
	
		for (HashMap<String, Object> info : list) {
			jobId = String.valueOf((int) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_ID));
			jobState = (int) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_STATE);
			jobType = (String) info.get(MariaDB.JOB_TABLE_COLUMN_JOB_TYPE);
			sourcePoint = (String) info.get(MariaDB.JOB_TABLE_COLUMN_SOURCE_POINT);
			targetPoint = (String) info.get(MariaDB.JOB_TABLE_COLUMN_TARGET_POINT);
			objectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_OBJECTS_COUNT);
			objectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_OBJECTS_SIZE);
			movedObjectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_MOVED_OBJECTS_COUNT);
			movedObjectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_MOVED_OBJECTS_SIZE);
			failedCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_FAILED_COUNT);
			failedSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_FAILED_SIZE);
			skipObjectsCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_SKIP_OBJECTS_COUNT);
			skipObjectsSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_SKIP_OBJECTS_SIZE);
			deleteObjectCount = (long) info.get(MariaDB.JOB_TABLE_COLUMN_DELETE_OBJECT_COUNT);
			deleteObjectSize = (long) info.get(MariaDB.JOB_TABLE_COLUMN_DELETE_OBJECT_SIZE);
			startTime = (String) info.get(MariaDB.JOB_TABLE_COLUMN_START);
			endTime = (String) info.get(MariaDB.JOB_TABLE_COLUMN_END);
			errorDesc = (String) info.get(MariaDB.JOB_TABLE_COLUMN_ERROR_DESC);
	
			if (jobState == STATE_REMOVE) {
				continue;
			}
	
			switch (jobState) {
			case STATE_INIT:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "INIT...", startTime));
				break;
				
			case STATE_MOVE:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "MOVE...", startTime));
				break;
				
			case STATE_COMPLETE:
				System.out.println(JOB_IS + String.format(FORMAT_START_END, jobId, "COMPLETED", startTime, endTime));
				break;
				
			case STATE_STOP:
				System.out.println(JOB_IS + String.format(FORMAT_START_END, jobId, "STOPPED", startTime, endTime));
				break;
				
			case STATE_RERUN:
				System.out.println(JOB_IS + String.format("%-5s\t%-15s%s", jobId, "RERUN INIT...", startTime));
				break;
				
			case STATE_RERUN_MOVE:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "RERUN MOVE", startTime));
				break;
				
			case STATE_ERROR:
				System.out.println(JOB_IS + String.format(FORMAT_START, jobId, "ERROR", startTime));
				break;
				
			default:
				break;
			}
			
			if (Repository.IFS_FILE.compareToIgnoreCase(jobType) == 0) {
				System.out.println("File : " + String.format("%s -> Object : %s", sourcePoint, targetPoint));
			} else {
				System.out.println("Object : " + String.format("%s -> Object : %s", sourcePoint, targetPoint));
			}
			
			if (jobState == STATE_ERROR) {
				System.out.println("Error : " + errorDesc);
				System.out.println();
				continue;
			}
	
			if (jobState == STATE_INIT) {
				unitSize = (double)objectsSize / UNIT_G;
				if (unitSize > 1.0) {
					System.out.println(String.format(FORMAT_G, PREPARE, objectsCount, unitSize));
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						System.out.println(String.format(FORMAT_M, PREPARE, objectsCount, unitSize));
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							System.out.println(String.format(FORMAT_K, PREPARE, objectsCount, unitSize));
						} else {
							System.out.println(String.format(FORMAT_B, PREPARE, objectsCount, objectsSize));
						}
					}
				}
			} else if (jobState == STATE_RERUN) {
				unitSize = (double)objectsSize / UNIT_G;
				if (unitSize > 1.0) {
					System.out.println(String.format(FORMAT_G, PREPARE, objectsCount, unitSize));
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						System.out.println(String.format(FORMAT_M, PREPARE, objectsCount, unitSize));
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							System.out.println(String.format(FORMAT_K, PREPARE, objectsCount, unitSize));
						} else {
							System.out.println(String.format(FORMAT_B, PREPARE, objectsCount, objectsSize));
						}
					}
				}
				
				if (skipObjectsCount > 0) {
					unitSkip = (double)skipObjectsSize / UNIT_G;
					if (unitSkip > 1.0) {
						System.out.println(String.format(FORMAT_G, SKIPPED, skipObjectsCount, unitSkip));
					} else {
						unitSkip = (double)skipObjectsSize / UNIT_M;
						if (unitSkip > 1.0) {
							System.out.println(String.format(FORMAT_M, SKIPPED, skipObjectsCount, unitSkip));
						} else {
							unitSkip = (double)skipObjectsSize / UNIT_K;
							if (unitSkip > 1.0) {
								System.out.println(String.format(FORMAT_K, SKIPPED, skipObjectsCount, unitSkip));
							} else {
								System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
				}
				
				if (failedCount > 0) {
					unitFailed = (double)failedSize / UNIT_G;
					if (unitFailed > 1.0) {
						System.out.println(String.format(FORMAT_G, FAILED, failedCount, unitFailed));
					} else {
						unitFailed = (double)failedSize / UNIT_M;
						if (unitFailed > 1.0) {
							System.out.println(String.format(FORMAT_M, FAILED, failedCount, unitFailed));
						} else {
							unitFailed = (double)failedSize / UNIT_K;
							if (unitFailed > 1.0) {
								System.out.println(String.format(FORMAT_K, FAILED, failedCount, unitFailed));
							} else {
								System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
				}
			} else {
				if (objectsCount == 0) {
					percent = 0.0;
				} else {
					percent = (((double)skipObjectsSize + (double)movedObjectsSize + (double)failedSize) / (double) objectsSize) * 100;
				}
				
				unitSize = (double)objectsSize / UNIT_G;
				
				if (unitSize > 1.0) {
					unitMove = (double)movedObjectsSize / UNIT_G;
					if (unitMove > 1.0) {
						System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fG", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
					} else {
						unitMove = (double) movedObjectsSize / UNIT_M;
						if (unitMove > 1.0) {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fM", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
						} else {
							unitMove = (double) movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fG\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						}
					}
				} else {
					unitSize = (double)objectsSize / UNIT_M;
					if (unitSize > 1.0) {
						unitMove = (double)movedObjectsSize / UNIT_M;
						if (unitMove > 1.0) {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10.2fM", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
						} else {
							unitMove = (double)movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fM\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						}
					} else {
						unitSize = (double)objectsSize / UNIT_K;
						if (unitSize > 1.0) {
							unitMove = (double)movedObjectsSize / UNIT_K;
							if (unitMove > 1.0) {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fK\n%-10s : %,14d/ %,10.2fK", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, unitMove));
							} else {
								System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10.2fK\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, unitSize, MOVED, movedObjectsCount, movedObjectsSize));
							}
						} else {
							System.out.println(String.format("%-10s : %14.2f%%\n%-10s : %,14d/ %,10dB\n%-10s : %,14d/ %,10dB", PROGRESS, percent, TOTAL, objectsCount, objectsSize, MOVED, movedObjectsCount, movedObjectsSize));
						}
					}
				}
				
				if (skipObjectsCount > 0) {
					unitSkip = (double)skipObjectsSize / UNIT_G;
					if (unitSkip > 1.0) {
						System.out.println(String.format(FORMAT_G, SKIPPED, skipObjectsCount, unitSkip));
					} else {
						unitSkip = (double)skipObjectsSize / UNIT_M;
						if (unitSkip > 1.0) {
							System.out.println(String.format(FORMAT_M, SKIPPED, skipObjectsCount, unitSkip));
						} else {
							unitSkip = (double)skipObjectsSize / UNIT_K;
							if (unitSkip > 1.0) {
								System.out.println(String.format(FORMAT_K, SKIPPED, skipObjectsCount, unitSkip));
							} else {
								System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, SKIPPED, skipObjectsCount, skipObjectsSize));
				}
				
				if (failedCount > 0) {
					unitFailed = (double)failedSize / UNIT_G;
					if (unitFailed > 1.0) {
						System.out.println(String.format(FORMAT_G, FAILED, failedCount, unitFailed));
					} else {
						unitFailed = (double)failedSize / UNIT_M;
						if (unitFailed > 1.0) {
							System.out.println(String.format(FORMAT_M, FAILED, failedCount, unitFailed));
						} else {
							unitFailed = (double)failedSize / UNIT_K;
							if (unitFailed > 1.0) {
								System.out.println(String.format(FORMAT_K, FAILED, failedCount, unitFailed));
							} else {
								System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
							}
						}
					}
				} else {
					System.out.println(String.format(FORMAT_B, FAILED, failedCount, failedSize));
				}

				if (deleteObjectCount > 0) {
					unitDelete = (double)deleteObjectSize / UNIT_G;
					if (unitDelete > 1.0) {
						System.out.println(String.format(FORMAT_G, DELETED, deleteObjectCount, unitDelete));
					} else {
						unitDelete = (double)deleteObjectSize / UNIT_M;
						if (unitDelete > 1.0) {
							System.out.println(String.format(FORMAT_M, DELETED, deleteObjectCount, unitDelete));
						} else {
							unitDelete = (double)deleteObjectSize / UNIT_K;
							if (unitDelete > 1.0) {
								System.out.println(String.format(FORMAT_K, DELETED, deleteObjectCount, unitDelete));
							} else {
								System.out.println(String.format(FORMAT_B, DELETED, deleteObjectCount, deleteObjectSize));
							}
						}
					}
				}
			} 
			System.out.println();
		}
	}

	private static void writeJobID(String jobID, String path) {
		if (path.length() < 20) {
			return;
		}

		String uuid = path.substring(7);
		File file = new File(JOBID_PATH + uuid);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file);
            fw.write(jobID);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }
	}
}

