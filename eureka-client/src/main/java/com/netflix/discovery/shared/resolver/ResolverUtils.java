/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.shared.resolver;

import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.shared.resolver.aws.AwsEndpoint;
import com.netflix.discovery.shared.transport.EurekaTransportConfig;
import com.netflix.discovery.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Tomasz Bak
 */
public final class ResolverUtils {

    private static final Logger logger = LoggerFactory.getLogger(ResolverUtils.class);

    private static final String LOCAL_IPV4_ADDRESS = SystemUtil.getServerIPv4();

    private static final Pattern ZONE_RE = Pattern.compile("(txt\\.)?([^.]+).*");

    private ResolverUtils() {
    }

    /**
     * @return returns two element array with first item containing list of endpoints from client's zone,
     *         and in the second list all the remaining ones
     */
    //返回两个List<AwsEndpoint>列表，第一个列表中的是和myZone属于同一个zone的EndPoint
    public static List<AwsEndpoint>[] splitByZone(List<AwsEndpoint> eurekaEndpoints, String myZone) {
        if (eurekaEndpoints.isEmpty()) {
            return new List[]{Collections.emptyList(), Collections.emptyList()};
        }
        if (myZone == null) {
            return new List[]{Collections.emptyList(), new ArrayList<>(eurekaEndpoints)};
        }
        List<AwsEndpoint> myZoneList = new ArrayList<>(eurekaEndpoints.size());
        List<AwsEndpoint> remainingZonesList = new ArrayList<>(eurekaEndpoints.size());
        for (AwsEndpoint endpoint : eurekaEndpoints) {
            if (myZone.equalsIgnoreCase(endpoint.getZone())) { // 是否为本地可用区
                myZoneList.add(endpoint);
            } else {
                remainingZonesList.add(endpoint);
            }
        }
        return new List[]{myZoneList, remainingZonesList};
    }

    public static String extractZoneFromHostName(String hostName) {
        Matcher matcher = ZONE_RE.matcher(hostName);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        return null;
    }

    /**
     * Randomize server list using local IPv4 address hash as a seed.
     *
     * @return a copy of the original list with elements in the random order
     */
    public static <T extends EurekaEndpoint> List<T> randomize(List<T> list) {
        // 数组大小为 0 或者 1 ，不进行打乱
        List<T> randomList = new ArrayList<>(list);
        if (randomList.size() < 2) {
            return randomList;
        }
        // 以本地IP为随机种子，有如下好处：
        // 多个主机，实现对同一个 EndPoint 集群负载均衡的效果。
        // 单个主机，同一个 EndPoint 集群按照固定顺序访问。Eureka-Server 不是强一致性的注册中心，Eureka-Client 对同一个 Eureka-Server 拉取注册信息，保证两者之间增量同步的一致性。
        Random random = new Random(LOCAL_IPV4_ADDRESS.hashCode());
        int last = randomList.size() - 1;
        for (int i = 0; i < last; i++) {
            int pos = random.nextInt(randomList.size() - i);
            if (pos != i) {
                Collections.swap(randomList, i, pos);
            }
        }
        return randomList;
    }

    /**
     * @return true if both list are the same, possibly in a different order
     */
    public static <T extends EurekaEndpoint> boolean identical(List<T> firstList, List<T> secondList) {
        if (firstList.size() != secondList.size()) {
            return false;
        }
        HashSet<T> compareSet = new HashSet<>(firstList);
        compareSet.removeAll(secondList);
        return compareSet.isEmpty();
    }

    public static AwsEndpoint instanceInfoToEndpoint(EurekaClientConfig clientConfig,
                                                     EurekaTransportConfig transportConfig,
                                                     InstanceInfo instanceInfo) {
        String zone = null;
        DataCenterInfo dataCenterInfo = instanceInfo.getDataCenterInfo();
        if (dataCenterInfo instanceof AmazonInfo) {
            zone = ((AmazonInfo) dataCenterInfo).get(AmazonInfo.MetaDataKey.availabilityZone);
        }

        String networkAddress;
        if (transportConfig.applicationsResolverUseIp()) {
            if (instanceInfo.getDataCenterInfo() instanceof AmazonInfo) {
                networkAddress = ((AmazonInfo) instanceInfo.getDataCenterInfo()).get(AmazonInfo.MetaDataKey.localIpv4);
            } else {
                networkAddress = instanceInfo.getIPAddr();
            }
        } else {
            networkAddress = instanceInfo.getHostName();
        }

        if (networkAddress == null) {  // final check
            logger.error("Cannot resolve InstanceInfo {} to a proper resolver endpoint, skipping", instanceInfo);
            return null;
        }

        return new AwsEndpoint(
                networkAddress,
                instanceInfo.getPort(),
                false,
                clientConfig.getEurekaServerURLContext(),
                clientConfig.getRegion(),
                zone
        );
    }
}
