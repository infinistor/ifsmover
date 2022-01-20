/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* ifsmover is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version 
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* ifsmover 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* ifsmover 개발팀은 사전 공지, 허락, 동의 없이 ifsmover 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package ifs_mover.repository;

import java.io.File;
import java.io.InputStream;

import com.amazonaws.services.s3.model.ObjectMetadata;

public class ObjectData {
    private ObjectMetadata meta;
    private InputStream is;
    private File file;
    private long size;

    public ObjectData() {
        meta = null;
        is = null;
        file = null;
    }

    public ObjectData(ObjectMetadata meta, InputStream is) {
        this.meta = meta;
        this.is = is;
    }

    public void setMetadata(ObjectMetadata metadata) {
        this.meta = metadata;
    }

    public void setInputStream(InputStream is) {
        this.is = is;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public ObjectMetadata getMetadata() {
        return meta;
    }

    public InputStream getInputStream() {
        return is;
    }

    public File getFile() {
        return file;
    }

    public long getSize() {
        return size;
    }
}
