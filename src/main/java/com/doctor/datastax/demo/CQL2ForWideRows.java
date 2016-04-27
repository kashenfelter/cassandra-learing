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

/**
 * @author sdcuike
 *
 *         Create At 2016年4月27日 下午2:20:29
 * 
 * @see http://www.datastax.com/dev/blog/cql3-for-cassandra-experts
 * 
 * 
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      1 | 2016-04-26 03:00:00.000000+0000 | 1
 * 
 *      (1 rows)
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      1 | 2016-04-26 03:00:00.000000+0000 | 2
 * 
 *      (1 rows)
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      1 | 2016-04-26 03:00:00.000000+0000 | 2
 * 
 *      (1 rows)
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      1 | 2016-04-26 03:00:00.000000+0000 | 2
 *      2 | 2016-04-26 03:00:00.000000+0000 | 1
 * 
 *      (2 rows)
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      1 | 2016-04-26 03:00:00.000000+0000 | 2
 *      2 | 2016-04-26 03:00:00.000000+0000 | 1
 * 
 *      (2 rows)
 *      cqlsh:email_statistic> DROP TABLE test_doctor.m_cf ;
 *      cqlsh:email_statistic> DROP TABLE test_doctor.m_cf ;
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      2 | 2016-04-26 03:00:00.000000+0000 | 4
 *      6 | 2016-04-26 03:00:00.000000+0000 | 1
 * 
 *      (2 rows)
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      2 | 2016-04-26 03:00:00.000000+0000 | 4
 *      6 | 2016-04-26 03:00:00.000000+0000 | 2
 * 
 *      (2 rows)
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      2 | 2016-04-26 03:00:00.000000+0000 | 4
 *      6 | 2016-04-26 03:00:00.000000+0000 | 2
 *      6 | 2016-04-26 04:00:00.000000+0000 | 1
 * 
 *      (3 rows)
 *      cqlsh:email_statistic> SELECT * from test_doctor.m_cf ;
 * 
 *      id | time | count
 *      ----+---------------------------------+-------
 *      2 | 2016-04-26 03:00:00.000000+0000 | 4
 *      6 | 2016-04-26 03:00:00.000000+0000 | 2
 *      6 | 2016-04-26 04:00:00.000000+0000 | 2
 * 
 *      (3 rows)
 * 
 */
public class CQL2ForWideRows {

    public static void main(String[] args) {
        CQL2ForWideRows demo = new CQL2ForWideRows();

        demo.connect("127.0.0.1");
        demo.createSchema();
        demo.loadData();
        demo.querySchema();
        demo.close();

    }

    protected Logger log = LoggerFactory.getLogger(getClass());
    private String address = "127.0.0.1";
    private Session session;
    private Cluster cluster;
    final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
        String createTable = "create columnfamily if not exists test_doctor.m_cf (" +
                "id bigint ,time timestamp,count counter,primary key(id,time)); ";
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

    public void close() {
        session.close();
        cluster.close();
    }
}
