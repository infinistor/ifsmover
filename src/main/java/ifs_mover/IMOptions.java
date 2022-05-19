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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class IMOptions {
	public enum WORK_TYPE {
		UNKNOWN, MOVE, COMPLETE, STOP, REMOVE, RERUN, STATUS, RERUN_MOVE, CHECK, ERROR
	}
	
	private String[] args;
	private WORK_TYPE workType;
	private Options options;
	private Config sourceConfig;
	private Config targetConfig;
	
	private boolean isType;
	private boolean isSourceConfPath;
	private boolean isTargetconfPath;
	private boolean isThread;
	private boolean isOption;
	private boolean isOptionEA;
	private boolean isOptionPerm;
	private boolean isOptionTime;
	private boolean isCheck;
	private boolean isStopId;
	private boolean isRemoveId;
	private boolean isRerunId;
	private boolean isStatus;
	
	private String type;
	private String sourceConfPath;
	private String targetConfPath;
	private String option;
	private String stopId;
	private String removeId;
	private String rerunId;
	private int threadCount;
	
	private final String FILE = "file";
	private final String S3 = "s3";
	private final String OS = "swift";
	private final String EA = "ea";
	private final String PERM = "perm";
	private final String TIME = "time";
	
	public IMOptions(String[] args) {
		this.args = args;
		
		sourceConfig = null;
		targetConfig = null;
		type = "";
		sourceConfPath = "";
		targetConfPath = "";
		option = "";
		stopId = "";
		removeId = "";
		rerunId = "";
		threadCount = 0;
		
		isType = false;
		isSourceConfPath = false;
		isTargetconfPath = false;
		isOption = false;
		isOptionEA = false;
		isOptionPerm = false;
		isOptionTime = false;
		isStopId = false;
		isRemoveId = false;
		isRerunId = false;
		isStatus = false;
	}
	
	public void handleOptions() {
		options = new Options();
		
		makeOptions();
		parseOptions();
		
		if (workType == WORK_TYPE.MOVE || workType == WORK_TYPE.RERUN || workType == WORK_TYPE.CHECK) {
			sourceConfig = new Config(sourceConfPath);
			targetConfig = new Config(targetConfPath);
			
			sourceConfig.configure();
			if (!checkSourceConfig()) {
				printUsage();
				return;
			}
			
			targetConfig.configure();
			if (!checkTargetConfig()) {
				printUsage();
			}
		}
	}
	
	private void makeOptions() {
		Option help = Option.builder("h").hasArg(false).required(false).build();
		Option type = Option.builder("t").hasArg(true).required(false).build();
		Option source = Option.builder(null).longOpt("source").hasArg(true).required(false).build();
		Option target = Option.builder(null).longOpt("target").hasArg(true).required(false).build();
		Option thread = Option.builder(null).longOpt("thread").hasArg(true).required(false).build();
		Option option = Option.builder("o").hasArg(true).required(false).build();
		Option stop = Option.builder(null).longOpt("jobstop").hasArg(true).required(false).build();
		Option remove = Option.builder(null).longOpt("jobremove").hasArg(true).required(false).build();
		Option rerun = Option.builder(null).longOpt("rerun").hasArg(true).required(false).build();
		Option check = Option.builder(null).longOpt("check").hasArg(false).required(false).build();
		Option status = Option.builder(null).longOpt("status").hasArg(false).required(false).build();
		
		options.addOption(help);
		options.addOption(type);
		options.addOption(source);
		options.addOption(target);
		options.addOption(thread);
		options.addOption(option);
		options.addOption(stop);
		options.addOption(remove);
		options.addOption(rerun);
		options.addOption(check);
		options.addOption(status);
	}
	
	private void parseOptions() {
		CommandLineParser parser = new DefaultParser();
		CommandLine line = null;

		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			printUsage();
		}

		isType = line.hasOption("t");
		if (isType) {
			type = line.getOptionValue("t");
			if (FILE.compareToIgnoreCase(type) != 0) {
				if (S3.compareToIgnoreCase(type) != 0) {
					if (OS.compareToIgnoreCase(type) != 0) {
						printUsage();
					}
				}
			}
		}

		isSourceConfPath = line.hasOption("source");
		if (isSourceConfPath) {
			sourceConfPath = line.getOptionValue("source");
			File file = new File(sourceConfPath);
			if (!file.exists()) {
				System.out.println("Can't find source config file : " + sourceConfPath);
				printUsage();
			}
		}
		
		isTargetconfPath = line.hasOption("target");
		if (isTargetconfPath) {
			targetConfPath = line.getOptionValue("target");
			File file = new File(targetConfPath);
			if (!file.exists()) {
				System.out.println("Can't find target config file : " + targetConfPath);
				printUsage();
			}
		}
		
		isThread = line.hasOption("thread");
		if (isThread) {
			String tmp = line.getOptionValue("thread");
			try {
				threadCount = Integer.parseInt(tmp);
			} catch (NumberFormatException e) {
				printUsage();
			}
		}
		
		isOption = line.hasOption("o");
		if (isOption) {
			option = line.getOptionValue("o");
		}
		
		isCheck = line.hasOption("check");
		
		isStopId = line.hasOption("jobstop");
		if (isStopId) {
			stopId = line.getOptionValue("jobstop");
		}
		
		isRemoveId = line.hasOption("jobremove");
		if (isRemoveId) {
			removeId = line.getOptionValue("jobremove");
		}
		
		isRerunId = line.hasOption("rerun");
		if (isRerunId) {
			rerunId = line.getOptionValue("rerun");
		}
		
		isStatus = line.hasOption("status");
		
		if (isType && isSourceConfPath && isTargetconfPath) {
			if (isCheck) {
				workType = WORK_TYPE.CHECK;
			} else if (isStopId || isRemoveId || isRerunId || isStatus) {
					printUsage();
			} else {
				workType = WORK_TYPE.MOVE;
				if (isOption) {
					checkOptionParameter();
				}
			}
		} else if (isRerunId && isSourceConfPath && isTargetconfPath) {
			if (isType || isStopId || isRemoveId || isStatus || isCheck) {
				printUsage();
			} else {
				workType = WORK_TYPE.RERUN;
			}
		} else if (isStopId || isRemoveId || isStatus) {
			if (isStopId && !isRemoveId && !isStatus) {
				workType = WORK_TYPE.STOP;
			} else if (!isStopId && isRemoveId && !isStatus) {
				workType = WORK_TYPE.REMOVE;
			} else if (!isStopId && !isRemoveId && isStatus) {
				workType = WORK_TYPE.STATUS;
			} else {
				printUsage();
			}
		} else {
			printUsage();
		}
	}
	
	private void printUsage() {
		workType = WORK_TYPE.UNKNOWN;
		
		System.out.println("Usage : ifs_mover [OPTION] ...");
		System.out.println("Move Objects");
		System.out.println("\t" + String.format("%-20s\t%s", "-t=file|s3|swift","source type, File(NAS etc) or S3 or Swift"));
		System.out.println("\t" + String.format("%-20s\t%s", "-source=source.conf", "source configuration file path"));
		System.out.println("\t" + String.format("%-20s\t%s", "-target=target.conf", "target configuration file path"));
		System.out.println("\t" + String.format("%-20s\t%s", "-o=ea,perm,time", "object meta info"));
		System.out.println("\t\t" + String.format("%-8s\t %s", "ea", "save fils's extented attribute in S3 meta"));
		System.out.println("\t\t" + String.format("%-8s\t %s", "perm", "save file's permission(rwxrwxrwx) in S3 meta"));
		System.out.println("\t\t" + String.format("%-8s\t %s", "", "744, READ permission granted to AUTHENTICATED_USER and PUBLIC"));
		System.out.println("\t\t" + String.format("%-8s\t %s", "time", "save file's C/M/A time in S3 meta"));
		System.out.println("\t" + String.format("%-20s\t%s", "-thread=", "thread count"));
		
		System.out.println("Stop Job");
		System.out.println("\t" + String.format("%-20s\t%s", "-jobstop=jobid", "stop a job in progress"));
		
		System.out.println("Remove Job");
		System.out.println("\t" + String.format("%-20s\t%s", "-jobremove=jobid", "delete stopped job information"));
		
		System.out.println("Rerun");
		System.out.println("\t" + String.format("%-20s\t%s", "-rerun=jobid", "function to execute only the DELTA part"));
		System.out.println("\t" + String.format("%-20s\t%s", "", "by performing it again based on the previously"));
		System.out.println("\t" + String.format("%-20s\t%s", "", "performed JOB information"));
		System.out.println("\t" + String.format("%-20s\t%s", "-source=source.conf", "source configuration file path"));
		System.out.println("\t" + String.format("%-20s\t%s", "-target=target.conf", "target configuration file path"));
		System.out.println("\t" + String.format("%-20s\t%s", "-thread=", "thread count"));
		
		System.out.println("Check");
		System.out.println("\t" + String.format("%-20s\t%s", "-check", "check source and target configuration"));
		System.out.println("\t" + String.format("%-20s\t%s", "-t=file|s3|swift","source type, FILE(NAS) or S3 or SWIFT"));
		System.out.println("\t" + String.format("%-20s\t%s", "-source=source.conf", "source configuration file path"));
		System.out.println("\t" + String.format("%-20s\t%s", "-target=target.conf", "target configuration file path"));
		
		System.out.println("Status Job");
		System.out.println("\t" + String.format("%-20s\t%s", "-status", "show job progress"));
		
		System.out.println("source.conf");
		System.out.println("\t" + String.format("%-20s\t%s", "mountpoint", "information mounted on the server to be performed"));
		System.out.println("\t" + String.format("%-20s\t%s", "", "mountpoint=/ means move all files"));
		System.out.println("\t" + String.format("%-20s\t%s", "endpoint", "http://ip:port"));
		System.out.println("\t" + String.format("%-20s\t%s", "access", "Access Key ID"));
		System.out.println("\t" + String.format("%-20s\t%s", "secret", "Secret Access Key"));
		System.out.println("\t" + String.format("%-20s\t%s", "bucket", "bucket name"));
		System.out.println("\t" + String.format("%-20s\t%s", "prefix", "PREFIX DIR name from which to start the MOVE"));
		System.out.println("\t" + String.format("%-20s\t%s", "move_size", "The size of the file that you can move at once."));
		System.out.println("\t" + String.format("%-20s\t%s", "user_name", "user name for swift"));
		System.out.println("\t" + String.format("%-20s\t%s", "api_key", "api key for swift"));
		System.out.println("\t" + String.format("%-20s\t%s", "auth_endpoint", "http://ip:port/v3, authentication endpoint for swift"));
		System.out.println("\t" + String.format("%-20s\t%s", "domain_id", "domain id for swift"));
		System.out.println("\t" + String.format("%-20s\t%s", "domain_name", "domain name for swift"));
		System.out.println("\t" + String.format("%-20s\t%s", "project_id", "project id for swift"));
		System.out.println("\t" + String.format("%-20s\t%s", "project_name", "project name for swift"));
		System.out.println("\t" + String.format("%-20s\t%s", "container", "list of containers(If it is empty, it means all container)"));

		System.out.println("target.conf");
		System.out.println("\t" + String.format("%-20s\t%s", "endpoint", "http://ip:port"));
		System.out.println("\t" + String.format("%-20s\t%s", "access", "Access Key ID"));
		System.out.println("\t" + String.format("%-20s\t%s", "secret", "Secret Access Key"));
		System.out.println("\t" + String.format("%-20s\t%s", "bucket", "bucket name"));
		System.out.println("\t" + String.format("%-20s\t%s", "prefix", "PREFIX DIR name from which to start the MOVED"));
		
		System.exit(-1);
	}
	
	public WORK_TYPE getWorkType() {
		return workType;
	}
	
	private void checkOptionParameter() {
		String[] opts = option.split(",");
		
		for (String str : opts) {
			if (EA.compareToIgnoreCase(str) == 0) {
				isOptionEA = true;
			} else if (PERM.compareToIgnoreCase(str) == 0) {
				isOptionPerm = true;
			} else if (TIME.compareToIgnoreCase(str) == 0) {
				isOptionTime = true;
			}
		}
	}
	
	public boolean hasOption() {
		return isOption;
	}
	
	public boolean hasOptionEA() {
		return isOptionEA;
	}
	
	public boolean hasOptionPerm() {
		return isOptionPerm;
	}
	
	public boolean hasOptionTime() {
		return isOptionTime;
	}
	
	public String getType() {
		return type;
	}
	
	public String getSourceMountPoint() {
		if (sourceConfig != null) {
			return sourceConfig.getMountPoint();
		}
		return null;
	}
	
	public String getSourceEndPoint() {
		if (sourceConfig != null) {
			return sourceConfig.getEndPoint();
		}
		return null;
	}
	
	public String getSourceAccssKey() {
		if (sourceConfig != null) {
			return sourceConfig.getAccessKey();
		}
		return null;
	}
	
	public String getSourceSecretKey() {
		if (sourceConfig != null) {
			return sourceConfig.getSecretKey();
		}
		return null;
	}
	
	public String getSourceBucket() {
		if (sourceConfig != null) {
			return sourceConfig.getBucket();
		}
		return null;
	}
	
	public String getSourcePrefix() {
		if (sourceConfig != null) {
			return sourceConfig.getPrefix();
		}
		return null;
	}

	public int getSourceMoveSize() {
		if (sourceConfig != null) {
			return Integer.parseInt(sourceConfig.getMoveSize());
		}
		return 0;
	}
	
	public String getTargetEndPoint() {
		if (targetConfig != null) {
			return targetConfig.getEndPoint();
		}
		return null;
	}
	
	public String getTargetAccessKey() {
		if (targetConfig != null) {
			return targetConfig.getAccessKey();
		}
		return null;
	}
	
	public String getTargetSecretKey() {
		if (targetConfig != null) {
			return targetConfig.getSecretKey();
		}
		return null;
	}
	
	public String getTargetBucket() {
		if (targetConfig != null) {
			return targetConfig.getBucket();
		}
		return null;
	}
	
	public boolean checkSourceConfig() {
		
		return true;
	}
	
	public int getThreadCount() {
		return threadCount;
	}
	
	public boolean checkTargetConfig() {
		
		return true;
	}
	
	public Config getSourceConfig() {
		return sourceConfig;
	}
	
	public Config getTargetConfig() {
		return targetConfig;
	}

	public String getStopId() {
		return stopId;
	}

	public String getRemoveId() {
		return removeId;
	}

	public String getRerunId() {
		return rerunId;
	}
}
