package com.doctor.datastax.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

/**
 * @author sdcuike
 *
 *         Create At 2016年4月27日 下午1:42:45
 * 
 * @see http://docs.datastax.com/en/latest-java-driver/java-driver/quick_start/qsSimpleClientCreate_t.html
 * 
 *      The complete code listing illustrates:
 *      connecting to a cluster
 *      retrieving metadata and printing it out
 *      closing the connection to the cluster
 */
public class ConnectingToACassandraCluster {
    protected static Logger log = LoggerFactory.getLogger(ConnectingToACassandraCluster.class);

    public static void main(String[] args) {
        String address = "127.0.0.1";

        try (Cluster cluster = Cluster.builder().addContactPoint(address).build();) {
            Metadata metadata = cluster.getMetadata();
            log.info("Connected to cluster:{}", metadata.getClusterName());

            for (Host host : metadata.getAllHosts()) {
                log.info("Datacenter:{},Host:{},Rack:{}", host.getDatacenter(), host.getAddress(), host.getRack());
            }
        }

    }

}
