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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.openstack4j.api.OSClient.OSClientV3;
import org.openstack4j.api.exceptions.AuthenticationException;
import org.openstack4j.api.exceptions.ResponseException;
import org.openstack4j.model.common.DLPayload;
import org.openstack4j.model.common.Identifier;
import org.openstack4j.model.common.header.Range;
import org.openstack4j.model.identity.v3.Token;
import org.openstack4j.model.storage.block.options.DownloadOptions;
import org.openstack4j.model.storage.object.SwiftContainer;
import org.openstack4j.model.storage.object.SwiftObject;
import org.openstack4j.model.storage.object.options.ContainerListOptions;
import org.openstack4j.model.storage.object.options.ObjectListOptions;
import org.openstack4j.openstack.OSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ifs_mover.Config;
import ifs_mover.DBManager;
import ifs_mover.Utils;

public class Swift implements Repository {
    private static final Logger logger = LoggerFactory.getLogger(Swift.class);
    private String jobId;
    private boolean isSource;
    private Config  config;
    private String errCode;
    private String errMessage;
    private Identifier domainIdentifier;
	private Identifier projectIdentifier;
	private OSClientV3 clientV3;
    private Token token;
    private List<String> bucketList = new ArrayList<String>();

    private final int CONTAINER_LIST_LIMIT = 10000;
    private final int OBJECT_LIST_LIMIT = 10000;
    private final int HTTP_STATUS_UNAUTHORIZED = 401;
    private final String X_STORAGE_POLICY = "X-Storage-Policy";
	private final String MULTIPART_INFO = "X-Object-Manifest";
	private final String X_TIMESTAMP = "X-Timestamp";
	private final String X_OPENSTACK_REQUEST_ID = "X-Openstack-Request-Id";
	private final String X_TRANS_ID = "X-Trans-Id";
	private final String X_OBJECT_META_FILE = "X-Object-Meta-File";
	private final String MTIME = "Mtime";
	private final String SEGMENTS = "_segments";

    Swift(String jobId) {
        this.jobId = jobId;
    }
    
    @Override
    public void setConfig(Config config, boolean isSource) {
        this.config = config;
        this.isSource = isSource;
    }

    @Override
    public int check(String type) {
        String domainId = config.getDomainId();
        if (domainId != null && !domainId.isEmpty()) {
            domainIdentifier = Identifier.byId(domainId);
        } else {
            String domainName = config.getDomainName();
            if (domainName != null && !domainName.isEmpty()) {
                domainIdentifier = Identifier.byName(domainName);
            } else {
                logger.error("Either domainId or donmainName must be entered.");
                errMessage = "Either domainId or donmainName must be entered.";
                return SWIFT_DOMAIN_VALUE_EMPTY;
            }
        }

        String projectId = config.getProjectId();
        if (projectId != null && !projectId.isEmpty()) {
            projectIdentifier = Identifier.byId(projectId);
        } else {
            String projectName = config.getProjectName();
            if (projectName != null && !projectName.isEmpty()) {
                projectIdentifier = Identifier.byName(projectName);
            } else {
                logger.error("Either projectId or projectName must be entered.");
                errMessage = "Either projectId or projectName must be entered.";
                return SWIFT_PROJECT_VALUE_EMPTY;
            }
        }

        clientV3 = getConnection();
        if (clientV3 == null) {
            return SWIFT_AUTH_ERROR;
        }

        List<String> listContainer = new ArrayList<String>();
        if (config.getContainer() != null && !config.getContainer().isEmpty()) {
            String[] conContainers = config.getContainer().split(",", 0);
            for (int i = 0; i < conContainers.length; i++) {
                listContainer.add(conContainers[i]);
            }
        }

        List<? extends SwiftContainer> containers = null;
        ContainerListOptions containerListOptions = ContainerListOptions.create().limit(CONTAINER_LIST_LIMIT);
        do {
            containers = containersList(containerListOptions);
            if (!containers.isEmpty()) {
                containerListOptions.marker(containers.get(containers.size() - 1).getName());
            }
            for (SwiftContainer container : containers) {
                if (container.getName().contains(SEGMENTS)) {
                    continue;
                }
                
                if (listContainer.size() > 0) {
                    boolean isCheck = false;
                    for (String containerName : listContainer) {
                        if (container.getName().equals(containerName)) {
                            isCheck = true;
                            break;
                        }
                    }
                    if (!isCheck) {
                        continue;
                    }
                } 

                if (!Utils.isValidBucketName(container.getName())) {
                    String containerName = Utils.getS3BucketName(container.getName());
                    if (!Utils.isValidBucketName(containerName)) {
                        logger.error("Container({}) name cannot be changed to S3 bucket name({}).", container.getName(), containerName);
                        errMessage = "Container(" + container.getName() + ") name cannot be changed to S3 bucket name(" + containerName + ").";
                        return SWIFT_INVALID_BUCKET_NAME;
                    } 
                }
            }
        } while (containers.size() >= CONTAINER_LIST_LIMIT);

        return NO_ERROR;
    }

    private OSClientV3 getConnection() {
        OSClientV3 client = null;
        try {
            OSFactory.enableHttpLoggingFilter(true);
            client = OSFactory.builderV3()
                .endpoint(config.getAuthEndpoint())
                .credentials(config.getUserName(), config.getApiKey(), domainIdentifier)
                .scopeToProject(projectIdentifier)
                .authenticate();
            token = client.getToken();
        } catch (AuthenticationException e) {
            if (e.getStatus() == HTTP_STATUS_UNAUTHORIZED) {
                logger.error("user name or api key / domain id(name) or project id(name) is invalid.");
                errMessage = "user name or api key / domain id(name) or project id(name) is invalid.";
                return null;
            }
            logger.error("{} - {}", e.getStatus(), e.getMessage());
            errMessage = e.getStatus() + " - " + e.getMessage();
            return null;
        } catch (ResponseException e) {
            if (e.getStatus() == 0) {
                errMessage = "auth-endpoint(" + config.getAuthEndpoint() + ") is invalid.";
                logger.error(errMessage);
                return null;
            }
            logger.error("{} - {}", e.getStatus(), e.getMessage());
            errMessage = e.getStatus() + " - " + e.getMessage();
            return null;
        }
        
        return client;
    }

    @Override
    public int init(String type) {
        String domainId = config.getDomainId();
        if (domainId != null && !domainId.isEmpty()) {
            domainIdentifier = Identifier.byId(domainId);
        } else {
            String domainName = config.getDomainName();
            if (domainName != null && !domainName.isEmpty()) {
                domainIdentifier = Identifier.byName(domainName);
            } else {
                logger.error("Either domainId or donmainName must be entered.");
                errMessage = "Either domainId or donmainName must be entered.";
                DBManager.insertErrorJob(jobId, errMessage);
                return SWIFT_DOMAIN_VALUE_EMPTY;
            }
        }

        String projectId = config.getProjectId();
        if (projectId != null && !projectId.isEmpty()) {
            projectIdentifier = Identifier.byId(projectId);
        } else {
            String projectName = config.getProjectName();
            if (projectName != null && !projectName.isEmpty()) {
                projectIdentifier = Identifier.byName(projectName);
            } else {
                logger.error("Either projectId or projectName must be entered.");
                errMessage = "Either projectId or projectName must be entered.";
                DBManager.insertErrorJob(jobId, errMessage);
                return SWIFT_PROJECT_VALUE_EMPTY;
            }
        }

        clientV3 = getConnection();
        if (clientV3 == null) {
            DBManager.insertErrorJob(jobId, errMessage);
            return SWIFT_AUTH_ERROR;
        }

        List<String> listContainer = new ArrayList<String>();
        if (config.getContainer() != null && !config.getContainer().isEmpty()) {
            String[] conContainers = config.getContainer().split(",", 0);
            for (int i = 0; i < conContainers.length; i++) {
                listContainer.add(conContainers[i]);
            }
        }

        List<? extends SwiftContainer> containers = null;
        ContainerListOptions containerListOptions = ContainerListOptions.create().limit(CONTAINER_LIST_LIMIT);
        do {
            containers = containersList(containerListOptions);
            if (!containers.isEmpty()) {
                containerListOptions.marker(containers.get(containers.size() - 1).getName());
            }

            for (SwiftContainer container : containers) {
                if (container.getName().contains(SEGMENTS)) {
                    continue;
                }
                
                if (listContainer.size() > 0) {
                    boolean isCheck = false;
                    for (String containerName : listContainer) {
                        if (container.getName().equals(containerName)) {
                            isCheck = true;
                            break;
                        }
                    }
                    if (!isCheck) {
                        continue;
                    }
                } 

                if (!Utils.isValidBucketName(container.getName())) {
                    String containerName = Utils.getS3BucketName(container.getName());
                    if (!Utils.isValidBucketName(containerName)) {
                        logger.error("Container({}) name cannot be changed to S3 bucket name({}).", container.getName(), containerName);
                        errMessage = "Container(" + container.getName() + ") name cannot be changed to S3 bucket name(" + containerName + ").";
                        DBManager.insertErrorJob(jobId, errMessage);
                        return SWIFT_INVALID_BUCKET_NAME;
                    }
                    bucketList.add(containerName);
                } else {
                    bucketList.add(container.getName());
                }
            }
        } while (containers.size() >= CONTAINER_LIST_LIMIT);

        return NO_ERROR;
    }

    @Override
    public boolean getVersioning() {
        return false;
    }

    @Override
    public void makeObjectList(boolean isRerun) {
        if (isRerun) {
			objectListRerun();
		} else {
			objectList();
		}
    }

    private List<? extends SwiftContainer> containersList(ContainerListOptions options) {
        while (true) {
            try {
                return clientV3.objectStorage().containers().list(options);
            } catch(Exception e) {
                clientV3 = getConnection();
            }
        }
    }

    private List<? extends SwiftObject> objectList(String container, ObjectListOptions options) {
        while (true) {
            try {
                return clientV3.objectStorage().objects().list(container, options);
            } catch (Exception e) {
                clientV3 = getConnection();
            }
        }
    } 

    private Map<String, String> getObjectMetadata(String container, String key) {
        while (true) {
            try {
                return clientV3.objectStorage().objects().getMetadata(container, key);
            } catch(Exception e) {
                clientV3 = getConnection();
            }
        }
    }

    private SwiftObject getObject(String container, String key) {
        while (true) {
            try {
                return clientV3.objectStorage().objects().get(container, key);
            } catch(Exception e) {
                clientV3 = getConnection();
            }
        }
    }
    
    private void objectList() {
        long count = 0;

        List<? extends SwiftContainer> containers = null;
        List<? extends SwiftObject> objs = null;
        Map<String, String> meta = null;

        List<String> listContainer = new ArrayList<String>();
        if (config.getContainer() != null && !config.getContainer().isEmpty()) {
            String[] configContainers = config.getContainer().split(",", 0);
            for (int i = 0; i < configContainers.length; i++) {
                listContainer.add(configContainers[i]);
            }
        }

        ContainerListOptions containerListOptions = ContainerListOptions.create().limit(CONTAINER_LIST_LIMIT);
        do {
            containers = containersList(containerListOptions);
            
            if (!containers.isEmpty()) {
                containerListOptions.marker(containers.get(containers.size() - 1).getName());
            }
            for (SwiftContainer container : containers) {
                if (container.getName().contains(SEGMENTS)) {
                    continue;
                }
                
                if (listContainer.size() > 0) {
                    boolean isCheck = false;
                    for (String containerName : listContainer) {
                        if (container.getName().equals(containerName)) {
                            isCheck = true;
                            break;
                        }
                    }
                    if (!isCheck) {
                        continue;
                    }
                }

                ObjectListOptions listOptions = ObjectListOptions.create().limit(OBJECT_LIST_LIMIT);
                if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
                    listOptions.startsWith(config.getPrefix());
                }

                do {
                    objs = objectList(container.getName(), listOptions);
                    
                    if (!objs.isEmpty()) {
                        listOptions.marker(objs.get(objs.size() - 1).getName());
                    }
                    
                    for (SwiftObject obj : objs) {
                        meta = getObjectMetadata(obj.getContainerName(), obj.getName());

                        long size = obj.getSizeInBytes();
                        JSONObject json = new JSONObject();
                        for (String key : meta.keySet()) {
                            if (key.equalsIgnoreCase(MULTIPART_INFO)
                                || key.equalsIgnoreCase(X_TIMESTAMP)
                                || key.equalsIgnoreCase(X_OPENSTACK_REQUEST_ID)
                                || key.equalsIgnoreCase(X_TRANS_ID)
                                || key.equalsIgnoreCase(MTIME)
                                || key.equalsIgnoreCase(X_OBJECT_META_FILE)) {
                                continue;
                            }
                            json.put(key, meta.get(key));
                        }

                        if (meta.get(MULTIPART_INFO) != null) {
                            String[] path = meta.get(MULTIPART_INFO).split("/", 2);
                            int i = 0;
                            SwiftObject object = null;
                            do {
                                String partPath = String.format("%08d", i++);
                                partPath = path[1] + partPath;
                                
                                object = getObject(path[0], partPath); 
                                if (object != null) {
                                    size += object.getSizeInBytes();
                                }
                            } while (object != null);
                        }
                        count++;
                        Utils.insertMoveObjectVersion(jobId, !obj.isDirectory(),  
                            obj.getLastModified().toString(),
                            size,
                            container.getName() + "/" + obj.getName(), 
                            "",
                            obj.getETag(),
                            meta.get(MULTIPART_INFO),
                            json.toString(),
                            false,
                            true);
                        Utils.updateJobInfo(jobId, size);
/*
                        OSClientV3 v3 = OSFactory.builderV3()
                            .endpoint(config.getAuthEndpoint())
                            .credentials(config.getUserName(), config.getApiKey(), domainIdentifier)
                            .scopeToProject(projectIdentifier)
                            .authenticate();
                        Map<String, String> map = new HashMap<String, String>();
                        map.put("uuid", UUID.randomUUID().toString());
                        map.put("File", UUID.randomUUID().toString());
                        v3.objectStorage().objects().updateMetadata(ObjectLocation.create(container.getName(), obj.getName()), map);
*/                        
                    }
                } while (objs.size() >= OBJECT_LIST_LIMIT);
            }
        } while (containers.size() >= CONTAINER_LIST_LIMIT);

        if (count == 0) {
            logger.error("Doesn't havn an object.");
            DBManager.insertErrorJob(jobId, "Doesn't havn an object.");
            System.exit(-1);
        }
    }

    private void objectListRerun() {
        long state = 0;

        List<? extends SwiftContainer> containers = null;
        List<? extends SwiftObject> objs = null;
        Map<String, String> meta = null;
        List<String> listContainer = new ArrayList<String>();
        if (config.getContainer() != null && !config.getContainer().isEmpty()) {
            String[] configContainers = config.getContainer().split(",", 0);
            for (int i = 0; i < configContainers.length; i++) {
                listContainer.add(configContainers[i]);
            }
        }
        ContainerListOptions containerListOptions = ContainerListOptions.create().limit(CONTAINER_LIST_LIMIT);
        do {
            containers = containersList(containerListOptions);

            if (!containers.isEmpty()) {
                containerListOptions.marker(containers.get(containers.size() - 1).getName());
            }
            for (SwiftContainer container : containers) {
                if (container.getName().contains(SEGMENTS)) {
                    continue;
                }
                
                if (listContainer.size() > 0) {
                    boolean isCheck = false;
                    for (String containerName : listContainer) {
                        if (container.getName().equals(containerName)) {
                            isCheck = true;
                            break;
                        }
                    }
                    if (!isCheck) {
                        continue;
                    }
                }

                ObjectListOptions listOptions = ObjectListOptions.create().limit(OBJECT_LIST_LIMIT);
                if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
                    listOptions.startsWith(config.getPrefix());
                }

                do {
                    objectList(container.getName(), listOptions);
                    if (!objs.isEmpty()) {
                        listOptions.marker(objs.get(objs.size() - 1).getName());
                    }
                    
                    for (SwiftObject obj : objs) {
                        meta = getObjectMetadata(obj.getContainerName(), obj.getName());
                        long size = obj.getSizeInBytes();
    
                        JSONObject json = new JSONObject();
                        for (String key : meta.keySet()) {
                            if (key.equalsIgnoreCase(MULTIPART_INFO)
                                || key.equalsIgnoreCase(X_TIMESTAMP)
                                || key.equalsIgnoreCase(X_OPENSTACK_REQUEST_ID)
                                || key.equalsIgnoreCase(X_TRANS_ID)
                                || key.equalsIgnoreCase(MTIME)
                                || key.equalsIgnoreCase(X_OBJECT_META_FILE)) {
                                continue;
                            }
                            json.put(key, meta.get(key));
                        }
                        
                        if (meta.get(MULTIPART_INFO) != null) {
                            String[] path = meta.get(MULTIPART_INFO).split("/", 2);
                            int i = 0;
                            SwiftObject object = null;
                            do {
                                String partPath = String.format("%08d", i++);
                                partPath = path[1] + partPath;
                                
                                object = getObject(path[0], partPath);
                                if (object != null) {
                                    size += object.getSizeInBytes();
                                }
                            } while (object != null);
                        }

                        Map<String, String> info = DBManager.infoExistObject(jobId, obj.getName());
                        if (info.isEmpty()) {
                            Utils.insertMoveObjectVersion(jobId, !obj.isDirectory(), 
                                obj.getLastModified().toString(),
                                size,
                                obj.getName(), 
                                "",
                                obj.getETag(),
                                meta.get(MULTIPART_INFO),
                                json.toString(),
                                false,
                                true);
                            Utils.updateJobInfo(jobId, size);
                        } else {
                            state = Integer.parseInt(info.get("object_state"));
                            String mTime = info.get("mtime");
                            if (state == 3 && mTime.compareTo(obj.getLastModified().toString()) == 0) {	
                                Utils.updateRerunSkipObjectVersion(jobId, obj.getName(), "");
                                Utils.updateJobRerunSkipInfo(jobId, size);
                            } else {
                                Utils.updateToMoveObjectVersion(jobId, obj.getLastModified().toString(), size, obj.getName(), "");
                                Utils.updateJobRerunInfo(jobId, size);
                            }
                        }
                    }
                } while (objs.size() >= OBJECT_LIST_LIMIT);
            }
        } while (containers.size() >= CONTAINER_LIST_LIMIT);
    }

    @Override
    public List<String> getBucketList() {
        return bucketList;
    }

    @Override
    public boolean createBuckets(List<String> list) {
        // not support
        return false;
    }

    @Override
    public String getErrCode() {
        return errCode;
    }

    @Override
    public String getErrMessage() {
        return errMessage;
    }

    @Override
    public String setPrefix(String path) {
        return null;
    }

    @Override
    public String setTargetPrefix(String path) {
        return null;
    }

    @Override
    public ObjectData getObject(String path) {
        return null;
    }

    @Override
    public ObjectData getObject(String bucket, String key, String versionId) {
        ObjectData data = new ObjectData();
        while (true) {
            try {
                OSClientV3 client = OSFactory.clientFromToken(token);
                SwiftObject object = client.objectStorage().objects().get(bucket, key);
                if (object != null) {
                    DLPayload load = object.download();
                    data.setInputStream(load.getInputStream());
                    data.setSize(object.getSizeInBytes());
                    return data;
                } else {
                    return null;
                }
            } catch (ResponseException e) {
                logger.warn("{}", e.getMessage());
                Utils.logging(logger, e);
                clientV3 = getConnection();
            }
        }
    }

    @Override
    public ObjectData getObject(String bucket, String key, String versionId, long start, long end) {
        ObjectData data = new ObjectData();
        DownloadOptions options = DownloadOptions.create();
        options.range(Range.from(start, end));
        while (true) {
            try {
                OSClientV3 client = OSFactory.clientFromToken(token);
                SwiftObject object = client.objectStorage().objects().get(bucket, key);
                DLPayload load = object.download(options);
                data.setInputStream(load.getInputStream());
                data.setSize(object.getSizeInBytes());
                break;
            } catch (Exception e) {
                logger.warn("{}", e.getMessage());
                Utils.logging(logger, e);
                clientV3 = getConnection();
            }
        }
        
        return data;
    }
}
