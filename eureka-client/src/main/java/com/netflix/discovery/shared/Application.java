/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.discovery.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.InstanceRegionChecker;
import com.netflix.discovery.provider.Serializer;
import com.netflix.discovery.util.StringCache;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The application class holds the list of instances for a particular
 * application.
 *
 * @author Karthik Ranganathan
 *
 * 应用
 *
 * 一个应用下，可以有多个应用实例对象
 */
@Serializer("com.netflix.discovery.converters.EntityBodyConverter")
@XStreamAlias("application")
@JsonRootName("application")
public class Application {
    
    private static Random shuffleRandom = new Random();

    @Override
    public String toString() {
        return "Application [name=" + name + ", isDirty=" + isDirty
                + ", instances=" + instances + ", shuffledInstances="
                + shuffledInstances + ", instancesMap=" + instancesMap + "]";
    }

    /**
     * 应用名
     */
    private String name;

    @XStreamOmitField
    private volatile boolean isDirty = false;

    /**
     * 应用实例集合
     */
    @XStreamImplicit
    private final Set<InstanceInfo> instances;

    /**
     * 打乱的应用实例集合{@link #instances}
     */
    private final AtomicReference<List<InstanceInfo>> shuffledInstances;

    /**
     * 应用实例映射
     * key：应用实例编号 {@link InstanceInfo#instanceId}
     */
    private final Map<String, InstanceInfo> instancesMap;

    public Application() {
        instances = new LinkedHashSet<InstanceInfo>();
        instancesMap = new ConcurrentHashMap<String, InstanceInfo>();
        shuffledInstances = new AtomicReference<List<InstanceInfo>>();
    }

    public Application(String name) {
        this();
        this.name = StringCache.intern(name);
    }

    @JsonCreator
    public Application(
            @JsonProperty("name") String name,
            @JsonProperty("instance") List<InstanceInfo> instances) {
        this(name);
        for (InstanceInfo instanceInfo : instances) {
            addInstance(instanceInfo);
        }
    }

    /**
     * Add the given instance info the list.
     *
     * @param i
     *            the instance info object to be added.
     */
    public void addInstance(InstanceInfo i) {
        // 添加到 应用实例映射
        instancesMap.put(i.getId(), i);
        synchronized (instances) {
            // 移除原有实例
            instances.remove(i);
            // 添加新实例
            instances.add(i);
            // 设置 isDirty ，目前只用于 `#toString()` 方法打印，无业务逻辑
            isDirty = true;
        }
    }

    /**
     * Remove the given instance info the list.
     *
     * @param i
     *            the instance info object to be removed.
     */
    public void removeInstance(InstanceInfo i) {
        removeInstance(i, true);
    }

    /**
     * Gets the list of instances associated with this particular application.
     * <p>
     * Note that the instances are always returned with random order after
     * shuffling to avoid traffic to the same instances during startup. The
     * shuffling always happens once after every fetch cycle as specified in
     * {@link EurekaClientConfig#getRegistryFetchIntervalSeconds}.
     * </p>
     *
     * @return the list of shuffled instances associated with this application.
     */
    @JsonProperty("instance")
    public List<InstanceInfo> getInstances() {
        return Optional.ofNullable(shuffledInstances.get()).orElseGet(this::getInstancesAsIsFromEureka);
    }

    /**
     * Gets the list of non-shuffled and non-filtered instances associated with this particular
     * application.
     *
     * @return list of non-shuffled and non-filtered instances associated with this particular
     *         application.
     */
    @JsonIgnore
    public List<InstanceInfo> getInstancesAsIsFromEureka() {
        synchronized (instances) {
           return new ArrayList<InstanceInfo>(this.instances);
        }
    }


    /**
     * Get the instance info that matches the given id.
     *
     * @param id
     *            the id for which the instance info needs to be returned.
     * @return the instance info object.
     */
    public InstanceInfo getByInstanceId(String id) {
        return instancesMap.get(id);
    }

    /**
     * Gets the name of the application.
     *
     * @return the name of the application.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the application.
     *
     * @param name
     *            the name of the application.
     */
    public void setName(String name) {
        this.name = StringCache.intern(name);
    }

    /**
     * @return the number of instances in this application
     */
    public int size() {
        return instances.size();
    }

    /**
     * Shuffles the list of instances in the application and stores it for
     * future retrievals.
     *
     * @param filterUpInstances
     *            indicates whether only the instances with status
     *            {@link InstanceStatus#UP} needs to be stored.
     */
    public void shuffleAndStoreInstances(boolean filterUpInstances) {
        _shuffleAndStoreInstances(filterUpInstances, false, null, null, null);
    }

    public void shuffleAndStoreInstances(Map<String, Applications> remoteRegionsRegistry,
                                         EurekaClientConfig clientConfig, InstanceRegionChecker instanceRegionChecker) {
        _shuffleAndStoreInstances(clientConfig.shouldFilterOnlyUpInstances(), true, remoteRegionsRegistry, clientConfig,
                instanceRegionChecker);
    }

    //随机打乱本Application中的实例列表
    //如果DataCenter是DataCenterInfo.Name.Amazon并且indexByRemoteRegions为true，则将本application中不属于本region
    //的instance分类存放到remoteRegionsRegistry中，并且从实例列表中删除
    private void _shuffleAndStoreInstances(boolean filterUpInstances, boolean indexByRemoteRegions,
                                           @Nullable Map<String, Applications> remoteRegionsRegistry,
                                           @Nullable EurekaClientConfig clientConfig,
                                           @Nullable InstanceRegionChecker instanceRegionChecker) {
        List<InstanceInfo> instanceInfoList;
        synchronized (instances) {
            instanceInfoList = new ArrayList<InstanceInfo>(instances);
        }
        boolean remoteIndexingActive = indexByRemoteRegions && null != instanceRegionChecker && null != clientConfig
                && null != remoteRegionsRegistry;
        if (remoteIndexingActive || filterUpInstances) {
            Iterator<InstanceInfo> it = instanceInfoList.iterator();
            while (it.hasNext()) {
                InstanceInfo instanceInfo = it.next();
                if (filterUpInstances && InstanceStatus.UP != instanceInfo.getStatus()) {
                    it.remove();
                } else if (remoteIndexingActive) {
                    //默认只对数据中心为DataCenterInfo.Name.Amazon的情况下有用
                    //获取实例region
                    String instanceRegion = instanceRegionChecker.getInstanceRegion(instanceInfo);
                    if (!instanceRegionChecker.isLocalRegion(instanceRegion)) {
                        Applications appsForRemoteRegion = remoteRegionsRegistry.get(instanceRegion);
                        if (null == appsForRemoteRegion) {
                            appsForRemoteRegion = new Applications();
                            remoteRegionsRegistry.put(instanceRegion, appsForRemoteRegion);
                        }

                        Application remoteApp =
                                appsForRemoteRegion.getRegisteredApplications(instanceInfo.getAppName());
                        if (null == remoteApp) {
                            remoteApp = new Application(instanceInfo.getAppName());
                            appsForRemoteRegion.addApplication(remoteApp);
                        }

                        remoteApp.addInstance(instanceInfo);
                        this.removeInstance(instanceInfo, false);
                        it.remove();
                    }
                }
            }

        }
        // 随机打乱
        Collections.shuffle(instanceInfoList, shuffleRandom);
        this.shuffledInstances.set(instanceInfoList);
    }

    private void removeInstance(InstanceInfo i, boolean markAsDirty) {
        // 移除 应用实例映射
        instancesMap.remove(i.getId());
        synchronized (instances) {
            // 移除 应用实例
            instances.remove(i);
            if (markAsDirty) {
                // 设置 isDirty ，目前只用于 `#toString()` 方法打印，无业务逻辑
                isDirty = true;
            }
        }
    }
}
