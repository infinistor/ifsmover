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

public class MoveData {
    private boolean isDelete;
    private boolean isLatest;
    private String path;
    private String versionId;
    private boolean isFile;
    private long size;
    private boolean skipCheck;
    private String ETag;
    private String multiPartInfo;
    private int objectState;
    private String mTime;

    public boolean isDelete() {
        return isDelete;
    }
    public void setDelete(boolean isDelete) {
        this.isDelete = isDelete;
    }
    public boolean isLatest() {
        return isLatest;
    }
    public void setLatest(boolean isLatest) {
        this.isLatest = isLatest;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
    public String getVersionId() {
        return versionId;
    }
    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }
    public boolean isFile() {
        return isFile;
    }
    public void setFile(boolean isFile) {
        this.isFile = isFile;
    }
    public long getSize() {
        return size;
    }
    public void setSize(long size) {
        this.size = size;
    }
    public boolean isSkipCheck() {
        return skipCheck;
    }
    public void setSkipCheck(boolean skipCheck) {
        this.skipCheck = skipCheck;
    }
    public String getETag() {
        return ETag;
    }
    public void setETag(String ETag) {
        this.ETag = ETag;
    }
    public String getMultiPartInfo() {
        return multiPartInfo;
    }
    public void setMultiPartInfo(String multiPartInfo) {
        this.multiPartInfo = multiPartInfo;
    }
    public int getObjectState() {
        return objectState;
    }
    public void setObjectState(int objectState) {
        this.objectState = objectState;
    }
    public String getmTime() {
        return mTime;
    }
    public void setmTime(String mTime) {
        this.mTime = mTime;
    }
}
