package com.doctor.datastax.demo;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

/**
 * @author sdcuike
 *         //TODO:
 *         Create At 2016年4月27日 下午4:33:37
 * 
 * @see http://docs.datastax.com/en/latest-java-driver/java-driver/reference/crudOperations.html
 * 
 */
public class ObjectMappingAPIDemo {

    public static void main(String[] args) {
        ObjectMappingAPIDemo demo = new ObjectMappingAPIDemo();

        demo.connect("127.0.0.1");
        demo.createSchema();
        Mapper<OpenStatistic> mapper = demo.getMapper();

    }

    protected Logger log = LoggerFactory.getLogger(getClass());
    private String address = "127.0.0.1";
    private Session session;
    private Cluster cluster;
    final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private Mapper<OpenStatistic> mapper;

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(address).build();
        Metadata metadata = cluster.getMetadata();
        log.info("Connected to cluster:{}", metadata.getClusterName());

        for (Host host : metadata.getAllHosts()) {
            log.info("Datacenter:{},Host:{},Rack:{}", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
        mapper = new MappingManager(session).mapper(OpenStatistic.class);
    }

    public void createSchema() {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS test_doctor WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1};";

        session.execute(createKeyspace);
        String createTable = "create columnfamily if not exists open_statistic (" +
                "email_recipient text ,open_time timestamp,count counter,primary key(email_recipient,open_time)); ";
        session.execute(createTable);

    }

    public void loadData() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime dateTime = LocalDateTime.of(now.getYear(), now.getMonth(), now.getDayOfMonth(), now.getHour(), 0, 0, 0);
        String date = dateTime.format(dateTimeFormatter);
        session.execute("update test_doctor.m_cf set count =count+1 where time = '" + date + "' and id = " + "6");
    }

    public void querySchema() {
        ResultSet results = session.execute("SELECT * FROM test_doctor.m_cf ");
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));

        for (Row row : results) {
            Date date = row.getTimestamp("time");
            LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.of("Asia/Shanghai"));
            System.out.println(row.getLong("id") + "  ===" + localDateTime.format(dateTimeFormatter) + "===" + row.getLong("count"));
        }

        System.out.println();
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime dateTime = LocalDateTime.of(now.getYear(), now.getMonth(), now.getDayOfMonth(), now.getHour(), 0, 0, 0);
        String date = dateTime.format(dateTimeFormatter);

        ResultSet set = session.execute("SELECT * FROM test_doctor.m_cf  where time = '" + date + "'  ALLOW FILTERING;");
        for (Row row : set) {
            Date date2 = row.getTimestamp("time");
            LocalDateTime localDateTime = LocalDateTime.ofInstant(date2.toInstant(), ZoneId.of("Asia/Shanghai"));
            System.out.println(row.getLong("id") + "  ===" + localDateTime.format(dateTimeFormatter) + "===" + row.getLong("count"));
        }
    }

    public Session getSession() {
        return session;
    }

    public Mapper<OpenStatistic> getMapper() {
        return mapper;
    }

    public void close() {
        session.close();
        cluster.close();
    }

}
