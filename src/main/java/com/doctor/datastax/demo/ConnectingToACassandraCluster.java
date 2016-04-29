package com.doctor.datastax.demo;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

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
 *      https://docs.datastax.com/en/cassandra/1.2/cassandra/security/security_config_native_authenticate_t.html
 *      https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_user_r.html
 *      https://docs.datastax.com/en/cql/3.0/cql/cql_reference/grant_r.html
 * 
 * 
 *      The complete code listing illustrates:
 *      connecting to a cluster
 *      retrieving metadata and printing it out
 *      closing the connection to the cluster
 */
public class ConnectingToACassandraCluster {
    protected static Logger log = LoggerFactory.getLogger(ConnectingToACassandraCluster.class);

    public static void main(String[] args) {
        String address = "dev015.qc.com:9042";
        String username = "email";
        String password = "email";
        String[] hosts = address.split(",");
        List<InetSocketAddress> inetAddress = new ArrayList<>(4);
        for (String host : hosts) {
            String[] hp = host.split(":");
            InetSocketAddress inetSocketAddress = new InetSocketAddress(hp[0], Integer.parseInt(hp[1]));
            inetAddress.add(inetSocketAddress);
        }

        try (Cluster cluster = Cluster.builder().addContactPointsWithPorts(inetAddress).withCredentials(username, password).build();) {
            Metadata metadata = cluster.getMetadata();
            log.info("Connected to cluster:{}", metadata.getClusterName());

            for (Host host : metadata.getAllHosts()) {
                log.info("Datacenter:{},Host:{},Rack:{}", host.getDatacenter(), host.getAddress(), host.getRack());
            }
        }

    }

}
