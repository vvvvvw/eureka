package com.netflix.discovery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.discovery.DefaultEurekaClientConfig.DEFAULT_ZONE;

/**
 * @author Nitesh Kant
 */
//远程region到可用区的映射关系
public abstract class AbstractAzToRegionMapper implements AzToRegionMapper {

    private static final Logger logger = LoggerFactory.getLogger(InstanceRegionChecker.class);
    private static final String[] EMPTY_STR_ARRAY = new String[0];

    protected final EurekaClientConfig clientConfig;

    /**
     * A default for the mapping that we know of, if a remote region is configured to be fetched but does not have
     * any availability zone mapping, we will use these defaults. OTOH, if the remote region has any mapping defaults
     * will not be used.
     */
    /** region到可用zone的映射的默认值，如果配置的需要获取的远程region没有任何可用zone映射，我们将使用这些默认值。否则，
     * 将不会被使用。 **/
    private final Multimap<String, String> defaultRegionVsAzMap =
            Multimaps.newListMultimap(new HashMap<String, Collection<String>>(), new Supplier<List<String>>() {
                @Override
                public List<String> get() {
                    return new ArrayList<String>();
                }
            });

    //Map<availablezone,region>
    private final Map<String, String> availabilityZoneVsRegion = new ConcurrentHashMap<String, String>();
    //当前配置的可供拉取的远程region
    private String[] regionsToFetch;

    protected AbstractAzToRegionMapper(EurekaClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        populateDefaultAZToRegionMap();
    }

    //返回 Map<zone,region>
    //如果对应region的availabilityZones为空或者是defaultZone的话，
    // 则会判断defaultRegionVsAzMap.containsKey(remoteRegion)，如果为false的话，进入的分支会抛出RuntimeException
    @Override
    public synchronized void setRegionsToFetch(String[] regionsToFetch) {
        if (null != regionsToFetch) {
            this.regionsToFetch = regionsToFetch;
            logger.info("Fetching availability zone to region mapping for regions {}", Arrays.toString(regionsToFetch));
            availabilityZoneVsRegion.clear();
            for (String remoteRegion : regionsToFetch) {
                Set<String> availabilityZones = getZonesForARegion(remoteRegion);
                if (null == availabilityZones
                        || (availabilityZones.size() == 1 && availabilityZones.contains(DEFAULT_ZONE))
                        || availabilityZones.isEmpty()) {
                    logger.info("No availability zone information available for remote region: " + remoteRegion
                            + ". Now checking in the default mapping.");
                    if (defaultRegionVsAzMap.containsKey(remoteRegion)) {
                        Collection<String> defaultAvailabilityZones = defaultRegionVsAzMap.get(remoteRegion);
                        for (String defaultAvailabilityZone : defaultAvailabilityZones) {
                            availabilityZoneVsRegion.put(defaultAvailabilityZone, remoteRegion);
                        }
                    } else {
                        String msg = "No availability zone information available for remote region: " + remoteRegion
                                + ". This is required if registry information for this region is configured to be "
                                + "fetched.";
                        logger.error(msg);
                        throw new RuntimeException(msg);
                    }
                } else {
                    for (String availabilityZone : availabilityZones) {
                        availabilityZoneVsRegion.put(availabilityZone, remoteRegion);
                    }
                }
            }

            logger.info("Availability zone to region mapping for all remote regions: {}", availabilityZoneVsRegion);
        } else {
            logger.info("Regions to fetch is null. Erasing older mapping if any.");
            availabilityZoneVsRegion.clear();
            this.regionsToFetch = EMPTY_STR_ARRAY;
        }
    }

    /**
     * Returns all the zones in the provided region.
     * @param region the region whose zones you want
     * @return a set of zones
     */
    //获取region的可用zone
    protected abstract Set<String> getZonesForARegion(String region);

    //根据zone名获取region名字
    @Override
    public String getRegionForAvailabilityZone(String availabilityZone) {
        String region = availabilityZoneVsRegion.get(availabilityZone);
        if (null == region) {
            return parseAzToGetRegion(availabilityZone);
        }
        return region;
    }

    //刷新 映射关系配置
    @Override
    public synchronized void refreshMapping() {
        logger.info("Refreshing availability zone to region mappings.");
        setRegionsToFetch(regionsToFetch);
    }

    /**
     * Tries to determine what region we're in, based on the provided availability zone.
     * @param availabilityZone the availability zone to inspect
     * @return the region, if available; null otherwise
     */
    /**
     * 尝试根据提供的可用区域确定我们所在的区域。其实就是假定 可用区域名就是在region名后面增加一个字符
     * @param availabilityZone 可用区域
     * @return
     */
    protected String parseAzToGetRegion(String availabilityZone) {
        // Here we see that whether the availability zone is following a pattern like <region><single letter>
        // If it is then we take ignore the last letter and check if the remaining part is actually a known remote
        // region. If yes, then we return that region, else null which means local region.
        if (!availabilityZone.isEmpty()) {
            String possibleRegion = availabilityZone.substring(0, availabilityZone.length() - 1);
            if (availabilityZoneVsRegion.containsValue(possibleRegion)) {
                return possibleRegion;
            }
        }
        return null;
    }

    private void populateDefaultAZToRegionMap() {
        defaultRegionVsAzMap.put("us-east-1", "us-east-1a");
        defaultRegionVsAzMap.put("us-east-1", "us-east-1c");
        defaultRegionVsAzMap.put("us-east-1", "us-east-1d");
        defaultRegionVsAzMap.put("us-east-1", "us-east-1e");

        defaultRegionVsAzMap.put("us-west-1", "us-west-1a");
        defaultRegionVsAzMap.put("us-west-1", "us-west-1c");

        defaultRegionVsAzMap.put("us-west-2", "us-west-2a");
        defaultRegionVsAzMap.put("us-west-2", "us-west-2b");
        defaultRegionVsAzMap.put("us-west-2", "us-west-2c");

        defaultRegionVsAzMap.put("eu-west-1", "eu-west-1a");
        defaultRegionVsAzMap.put("eu-west-1", "eu-west-1b");
        defaultRegionVsAzMap.put("eu-west-1", "eu-west-1c");
    }
}
