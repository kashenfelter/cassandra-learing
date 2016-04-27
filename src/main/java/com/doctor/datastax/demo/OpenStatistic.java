package com.doctor.datastax.demo;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;

/**
 * @author sdcuike
 *
 *         Create At 2016年4月27日 下午4:36:26
 */
@Table(keyspace = "test_doctor", name = "open_statistic")
public class OpenStatistic {
    @Transient
    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    @Column(name = "email_recipient")
    private String emailRecipient;

    @Column(name = "open_time")
    private Date openTime;

    private Long count;

    public String getEmailRecipient() {
        return emailRecipient;
    }

    public void setEmailRecipient(String emailRecipient) {
        this.emailRecipient = emailRecipient;
    }

    public Date getOpenTime() {
        return openTime;
    }

    public void setOpenTime(Date openTime) {
        this.openTime = openTime;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object obj) {

        return EqualsBuilder.reflectionEquals(this, obj, false);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, false);
    }

    @Override
    public String toString() {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(openTime.toInstant(), ZoneId.of("Asia/Shanghai"));
        return "emailRecipient:" + emailRecipient + ";openTime:" + localDateTime.format(dateTimeFormatter) + ";count:" + count;
    }

    @Accessor
    public interface OpenStatisticAccessor {

        @Query("update test_doctor.open_statistic set count =count+1 where open_time = :openTime and email_recipient = :emailRecipient")
        ResultSet add(@Param("openTime") Date openTime, @Param("emailRecipient") String emailRecipient);

        @Query("SELECT * FROM test_doctor.open_statistic")
        public Result<OpenStatistic> getAll();

        @Query("SELECT * FROM test_doctor.open_statistic where open_time =:openTime ALLOW FILTERING;")
        public Result<OpenStatistic> getAll(@Param("openTime") Date openTime);
    }
}
