package com.doctor.datastax.demo;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.doctor.datastax.demo.OpenStatistic.OpenStatisticAccessor;

/**
 * Object-mapping API demo
 * 
 * @author sdcuike
 *         //TODO:
 *         Create At 2016年4月27日 下午4:33:37
 * 
 * @see http://docs.datastax.com/en/latest-java-driver/java-driver/reference/crudOperations.html
 *      http://docs.datastax.com/en/latest-java-driver/common/drivers/reference/accessorAnnotatedInterfaces.html
 * 
 */
public class ObjectMappingAPIDemo {

    public static void main(String[] args) {
        ObjectMappingAPIDemo demo = new ObjectMappingAPIDemo();
        demo.connect("127.0.0.1");
        demo.createSchema();
        OpenStatisticAccessor openStatisticAccessor = demo.getOpenStatisticAccessor();
        System.out.println("=========");
        Result<OpenStatistic> all = openStatisticAccessor.getAll();
        all.forEach(System.out::println);
        System.out.println("=========");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime openTime = LocalDateTime.of(now.getYear(), now.getMonth(), now.getDayOfMonth(), now.getHour(), 0, 0, 0);
        Date openTimeT = Date.from(openTime.toInstant(ZoneOffset.ofHours(8)));
        openStatisticAccessor.add(openTimeT, "test");

        System.out.println("=========");
        all = openStatisticAccessor.getAll();
        all.forEach(System.out::println);

        System.out.println("=========");
        all = openStatisticAccessor.getAll(openTimeT);
        all.forEach(System.out::println);
        demo.close();
    }

    protected Logger log = LoggerFactory.getLogger(getClass());
    private String address = "127.0.0.1";
    private Session session;
    private Cluster cluster;
    final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private OpenStatisticAccessor openStatisticAccessor;

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(address).build();
        Metadata metadata = cluster.getMetadata();
        log.info("Connected to cluster:{}", metadata.getClusterName());

        for (Host host : metadata.getAllHosts()) {
            log.info("Datacenter:{},Host:{},Rack:{}", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();

    }

    public void createSchema() {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS test_doctor WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1};";

        session.execute(createKeyspace);
        String createTable = "create columnfamily if not exists test_doctor.open_statistic (" +
                "email_recipient text ,open_time timestamp,count counter,primary key(email_recipient,open_time)); ";
        session.execute(createTable);
        MappingManager mappingManager = new MappingManager(session);
        openStatisticAccessor = mappingManager.createAccessor(OpenStatisticAccessor.class);

    }

    public Session getSession() {
        return session;
    }

    public OpenStatisticAccessor getOpenStatisticAccessor() {
        return openStatisticAccessor;
    }

    public void close() {
        session.close();
        cluster.close();
    }

}
