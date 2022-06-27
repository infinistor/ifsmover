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
package ifs_mover.repository;

import java.util.List;

import com.amazonaws.services.s3.model.ObjectMetadata;

import ifs_mover.Config;

public interface Repository {
    public final String IFS_FILE = "file";
    public final String S3 = "s3";
    public final String SWIFT = "swift";

    public final int NO_ERROR = 0;
    public final int FILE_PATH_NOT_EXIST = -1000;
    public final int FILE_PATH_NOT_DIR = -1001;

    public final int ENDPOINT_IS_NULL = -2000;
    public final int BUCKET_IS_NULL = -2001;
    public final int UNABLE_FIND_REGION = -2002;
    public final int INVALID_ENDPOINT = -2003;
    public final int INVALID_ACCESS_KEY = -2004;
    public final int INVALID_SECRET_KEY = -2005;
    public final int AMAZON_CLIENT_EXCEPTION = -2006;
    public final int AMAZON_SERVICE_EXCEPTION = -2007;
    public final int BUCKET_NO_EXIST = -2008;
    public final int FAILED_CREATE_BUCKET = -2009;
    public final int ACCESS_DENIED_ERROR = -2010;

    public final int SWIFT_DOMAIN_VALUE_EMPTY = -3000;
    public final int SWIFT_PROJECT_VALUE_EMPTY = -3001;
    public final int SWIFT_AUTH_ERROR = -3002;
    public final int SWIFT_RESPONSE_ERROR = -3003;
    public final int SWIFT_INVALID_BUCKET_NAME = -3004;

    String getErrCode();
    String getErrMessage();
    void setConfig(Config config, boolean isSource);
    int check(String type);
    int init(String type);
    boolean isVersioning();
    String getVersioningStatus();
    
    List<String> getBucketList();
    boolean createBuckets(List<String> list);
    void makeObjectList(boolean isRerun, boolean targetVersioning);
    String setPrefix(String path);
    String setTargetPrefix(String path);

    ObjectMetadata getMetadata(String bucket, String key, String versionId);
    ObjectData getObject(String path);
    ObjectData getObject(String bucket, String key, String versionId);
    ObjectData getObject(String bucket, String key, String versionId, long start);
    ObjectData getObject(String bucket, String key, String versionId, long start, long end);
}
