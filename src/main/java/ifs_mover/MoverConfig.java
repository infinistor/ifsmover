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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoverConfig {
    private Properties properties;
    
    private static String dbRepository;
    private String dbHost;
	private String database;
	private String dbPort;
	private String dbUser;
	private String dbPass;
	private int dbPoolSize;
    private String replaceChars;
    private boolean isSetTagetPathToLowerCase;

    private static final Logger logger = LoggerFactory.getLogger(MoverConfig.class);

    private static final String CONFIG_PATH = "/usr/local/pspace/etc/mover/ifs-mover.conf";
    private static final String DB_REPOSITORY = "db_repository";
    private static final String DB_HOST = "db_host";
    private static final String DB_NAME = "db_name";
    private static final String DB_PORT = "db_port";
    private static final String DB_USER = "db_user";
    private static final String DB_PASSWORD = "db_password";
    private static final String DB_POOL_SIZE = "db_pool_size";
    public static final String MARIADB = "mariadb";
    public static final String SQLITEDB = "sqlite";
    public static final String REPLACE_CHARS = "replace_chars";
    public static final String SET_TARGET_PATH_TO_LOWERCASE = "set_targfet_path_to_lowercase";

    private static final String LOG_CONFIG_NOT_EXIST = "config file is not exist.";
    private static final String LOG_CONFIG_FAILED_LOADING = "config file loading is failed.";
    private static final String LOG_CONFIG_MUST_CONTAIN = "config file must contain: ";

    public static MoverConfig getInstance() {
		return LazyHolder.INSTANCE;
	}

	private static class LazyHolder {
		private static final MoverConfig INSTANCE = new MoverConfig();
	}

    private MoverConfig() {
        String path = System.getProperty("configure");
		if (path == null) {
			path = CONFIG_PATH;
		}
        logger.info(path);
        properties = new Properties();
        try (InputStream myis = new FileInputStream(path)) {
            properties.load(myis);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(LOG_CONFIG_NOT_EXIST);
        } catch (IOException e) {
            throw new IllegalArgumentException(LOG_CONFIG_FAILED_LOADING);
        }
    }

    public void configure() {
        dbRepository = properties.getProperty(DB_REPOSITORY);

        dbHost = properties.getProperty(DB_HOST);
        if (dbRepository == null) {
            if (dbHost == null) {
                throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_HOST);
            }
        } else {
            if (dbRepository.equalsIgnoreCase(MARIADB)) {
                if (dbHost == null) {
                    throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_HOST);
                }
            }
        }

        database = properties.getProperty(DB_NAME);
        if (dbRepository == null) {
            if (database == null) {
                throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_NAME);
            }
        } else {
            if (dbRepository.equalsIgnoreCase(MARIADB)) {
                if (database == null) {
                    throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_NAME);
                }
            }
        }

        dbPort = properties.getProperty(DB_PORT);
        if (dbRepository == null) {
            if (dbPort == null) {
                throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_PORT);
            }
        } else {
            if (dbRepository.equalsIgnoreCase(MARIADB)) {
                if (dbPort == null) {
                    throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_PORT);
                }
            }
        }

        dbUser = properties.getProperty(DB_USER);
        if (dbRepository == null) {
            if (dbUser == null) {
                throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_USER);
            }
        } else {
            if (dbRepository.equalsIgnoreCase(MARIADB)) {
                if (dbUser == null) {
                    throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_USER);
                }
            }
        }

        dbPass = properties.getProperty(DB_PASSWORD);
        if (dbRepository == null) {
            if (dbPass == null) {
                throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_PASSWORD);
            }
        } else {
            if (dbRepository.equalsIgnoreCase(MARIADB)) {
                if (dbPass == null) {
                    throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_PASSWORD);
                }
            }
        }
        

        String dbPoolSize = properties.getProperty(DB_POOL_SIZE);
        if (dbRepository == null) {
            if (dbPoolSize == null) {
                throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_POOL_SIZE);
            } else {
                this.dbPoolSize = Integer.parseInt(dbPoolSize);
            }
        } else {
            if (dbRepository.equalsIgnoreCase(MARIADB)) {
                if (dbPoolSize == null) {
                    throw new IllegalArgumentException(LOG_CONFIG_MUST_CONTAIN + DB_POOL_SIZE);
                } else {
                    this.dbPoolSize = Integer.parseInt(dbPoolSize);
                }
            }
        }

        if (dbRepository == null) {
            dbRepository = MARIADB;
        }

        String replaceCharsTemp = properties.getProperty(REPLACE_CHARS);
        if (replaceCharsTemp != null) {
            logger.debug("REPLACE_CHARS: {}", replaceCharsTemp);
            
            // + [ ]
            replaceChars = "[ ";
            int index = replaceCharsTemp.indexOf("+");
            if (index != -1) {
                replaceChars += "+";
            }
            index = replaceCharsTemp.indexOf("$");
            if (index != -1) {
                replaceChars += "$";
            }
            index = replaceCharsTemp.indexOf("|");
            if (index != -1) {
                replaceChars += "|";
            }

            // + \\
            index = replaceCharsTemp.indexOf("(");
            if (index != -1) {
                replaceChars += "\\(";
            }
            index = replaceCharsTemp.indexOf(")");
            if (index != -1) {
                replaceChars += "\\)";
            }
            index = replaceCharsTemp.indexOf("{");
            if (index != -1) {
                replaceChars += "\\{";
            }
            index = replaceCharsTemp.indexOf("}");
            if (index != -1) {
                replaceChars += "\\}";
            }
            index = replaceCharsTemp.indexOf("[");
            if (index != -1) {
                replaceChars += "\\[";
            }
            index = replaceCharsTemp.indexOf("]");
            if (index != -1) {
                replaceChars += "\\]";
            }
            index = replaceCharsTemp.indexOf("^");
            if (index != -1) {
                replaceChars += "\\^";
            }

            index = replaceCharsTemp.indexOf("\"");
            if (index != -1) {
                replaceChars += "\"";
            }

            index = replaceCharsTemp.indexOf("\\");
            // logger.info("find \\ : {}", index);
            if (index != -1) {
                replaceChars += "\\\\";
            }

            // Add
            index = replaceCharsTemp.indexOf("@");
            if (index != -1) {
                replaceChars += "@";
            }
            index = replaceCharsTemp.indexOf(",");
            if (index != -1) {
                replaceChars += ",";
            }
            index = replaceCharsTemp.indexOf("=");
            if (index != -1) {
                replaceChars += "=";
            }
            index = replaceCharsTemp.indexOf("?");
            if (index != -1) {
                replaceChars += "?";
            }
            index = replaceCharsTemp.indexOf("!");
            if (index != -1) {
                replaceChars += "!";
            }
            index = replaceCharsTemp.indexOf("#");
            if (index != -1) {
                replaceChars += "#";
            }
            index = replaceCharsTemp.indexOf(";");
            if (index != -1) {
                replaceChars += ";";
            }
            index = replaceCharsTemp.indexOf(":");
            if (index != -1) {
                replaceChars += ":";
            }
            index = replaceCharsTemp.indexOf("`");
            if (index != -1) {
                replaceChars += "`";
            }
            index = replaceCharsTemp.indexOf("~");
            if (index != -1) {
                replaceChars += "~";
            }
            index = replaceCharsTemp.indexOf("%");
            if (index != -1) {
                replaceChars += "%";
            }
            index = replaceCharsTemp.indexOf("&");
            if (index != -1) {
                replaceChars += "&";
            }
            index = replaceCharsTemp.indexOf("<");
            if (index != -1) {
                replaceChars += "<";
            }
            index = replaceCharsTemp.indexOf(">");
            if (index != -1) {
                replaceChars += ">";
            }
            replaceChars += "]";
        }

        String lowerString = properties.getProperty(SET_TARGET_PATH_TO_LOWERCASE);
        if (lowerString != null) {
            try {
                int value = Integer.parseInt(lowerString);
                if (value == 0) {
                    isSetTagetPathToLowerCase = false;
                } else if (value == 1) {
                    isSetTagetPathToLowerCase = true;
                } else {
                    isSetTagetPathToLowerCase = false;
                }
            } catch (NumberFormatException e) {
                isSetTagetPathToLowerCase = false;
            }
        } else {
            isSetTagetPathToLowerCase = false;
        }
    }

    public static String getDBRepository() {
        return dbRepository;
    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDbPort() {
        return dbPort;
    }

    public void setDbPort(String dbPort) {
        this.dbPort = dbPort;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPass() {
        return dbPass;
    }

    public void setDbPass(String dbPass) {
        this.dbPass = dbPass;
    }

    public int getDbPoolSize() {
        return dbPoolSize;
    }

    public void setDbPoolSize(int dbPoolSize) {
        this.dbPoolSize = dbPoolSize;
    }

    public String getReplaceChars() {
        return replaceChars;
    }

    public boolean isSetTagetPathToLowerCase() {
        return isSetTagetPathToLowerCase;
    }
}
