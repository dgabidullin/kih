package net.dag.kih.unit;

import net.dag.kih.service.avro.AvroConverter;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


@RunWith(SpringRunner.class)
@ActiveProfiles(profiles = {"test"})
public class AvroConverterTest {

  @Autowired
  private AvroConverter avroConverter;

  @Test
  public void decode1() throws IOException {
    String[] avroRecords = {
        "[INSERT,1,yandex,2019-09-12 21:00:00.0,2019-09-12 21:00:00.0]",
        "[INSERT,1,google,2019-09-25 21:00:00.0,null]"
    };
    byte[] content = FileUtils.readFileToByteArray(ResourceUtils.getFile("classpath:data/domains2.avro"));
    String schemaStr = "{\"type\": \"record\", " +
        "\"version\": \"segment_domains2_1569411343094\", " +
        "\"name\": \"segment_domains2\", " +
        "\"namespace\": \"loadRangeOffsets.rdm\", " +
        "\"fields\": [" +
        "{\"type\": \"string\", \"name\": \"action\"}, " +
        "{\"type\": [\"int\"], \"name\": \"segment_id\"}, " +
        "{\"type\": [\"string\"], \"name\": \"domain2\"}, " +
        "{\"type\": [{\"logicalType\": \"timestamp-millis\", \"type\": \"long\"}], \"name\": \"dt_begin\"}, " +
        "{\"type\": [\"null\", {\"logicalType\": \"timestamp-millis\", \"type\": \"long\"}], \"name\": \"dt_end\"}]}";

    List<Row> rows = avroConverter.decode(schemaStr, content);

    assertArrayEquals(avroRecords, rows.stream().map(Row::toString).toArray());
  }

  @Test
  public void decode() throws Exception {
    String[] avroRecords = {
        "[7832,1,100,0.001,0.25,4.0,0,1044,1043.9999999999989,5940485.0]",
        "[7832,2,100,0.001,0.3,2.8,0,374,373.99999999999864,2176060.0]",
        "[7832,3,100,0.001,0.25,4.0,0,2586,2586.0032944695863,9734110.0]",
        "[7832,5,100,0.001,0.25,3.1,0,1137,1137.0000406099698,1.1355978E7]"
    };

    byte[] content = FileUtils.readFileToByteArray(ResourceUtils.getFile("classpath:data/avro_record.avro"));
    String schemaStr = "{\"type\":\"record\",\"name\":\"WeightLog\",\"namespace\":\"loadRangeOffsets.tvi\",\"fields\":[{\"name\":\"wlgDay\",\"type\":\"int\"},{\"name\":\"wlgUnion\",\"type\":\"int\"},{\"name\":\"wlgSetIteration\",\"type\":[\"int\"]},{\"name\":\"wlgSetAccuracy\",\"type\":[\"double\"]},{\"name\":\"wlgSetMin\",\"type\":[\"double\"]},{\"name\":\"wlgSetMax\",\"type\":[\"double\"]},{\"name\":\"wlgIterations\",\"type\":[\"int\"]},{\"name\":\"wlgSample\",\"type\":[\"int\"]},{\"name\":\"wlgWeiSum\",\"type\":[\"double\"]},{\"name\":\"wlgUniverse\",\"type\":[\"null\",\"double\"]}],\"version\":\"WeightLog_1554737196843_358_20190311\",\"BatchID\":358,\"DataDate\":\"20190311\"}";
    List<Row> rows = avroConverter.decode(schemaStr, content);
    assertEquals(4, rows.size());
    assertArrayEquals(avroRecords, rows.stream().map(Row::toString).toArray());
  }

  @Test
  public void decode2() throws Exception {
    String avroRecord = "[INSERT,32531,32531,207,false,2019-04-03 21:06:46.097,22221,2019-04-01,6623,7166,2019-04-02 02:00:00.0,86400,7166,2019-04-02 02:00:00.0,6623,7166,2019-04-02 02:00:00.0]";
    byte[] content = FileUtils.readFileToByteArray(ResourceUtils.getFile("classpath:data/avro_record2.avro"));
    String schemaStr = "{\"type\": \"record\", \"version\": \"TvMappingProtocol_1555948100124_207_2019-04-02\", \"BatchID\": 207, \"DataDate\": \"2019-04-02\", \"name\": \"TvMappingProtocol\", \"namespace\": \"loadRangeOffsets.monitoring\", \"fields\": [{\"name\":\"action\",\"type\":\"string\"},{\"type\": [\"int\"], \"name\": \"VersionID\"}, {\"type\": [\"int\"], \"name\": \"TaskID\"}, {\"type\": [\"int\"], \"name\": \"BatchID\"}, {\"type\": [\"boolean\"], \"name\": \"Reload\"}, {\"type\": [{\"logicalType\": \"timestamp-millis\", \"type\": \"long\"}], \"name\": \"SysTime\"}, {\"type\": [\"int\"], \"name\": \"LoadID\"}, {\"type\": [{\"logicalType\": \"date\", \"type\": \"int\"}], \"name\": \"MapDate\"}, {\"type\": [\"int\"], \"name\": \"ChannelID\"}, {\"type\": [\"int\"], \"name\": \"CompanyID\"}, {\"type\": [{\"logicalType\": \"timestamp-millis\", \"type\": \"long\"}], \"name\": \"StartTime\"}, {\"type\": [\"int\"], \"name\": \"Duration\"}, {\"type\": [\"int\"], \"name\": \"LocalCompanyID\"}, {\"type\": [{\"logicalType\": \"timestamp-millis\", \"type\": \"long\"}], \"name\": \"LocalTime\"}, {\"type\": [\"int\"], \"name\": \"OriginalChannelID\"}, {\"type\": [\"int\"], \"name\": \"OriginalCompanyID\"}, {\"type\": [{\"logicalType\": \"timestamp-millis\", \"type\": \"long\"}], \"name\": \"OriginalTime\"}]}";

    List<Row> rows = avroConverter.decode(schemaStr, content);

    assertEquals(1, rows.size());
    assertEquals(avroRecord, rows.get(0).toString());
  }

  @Test
  public void testTargets() throws Exception {
    String avroRecord = "[1,1592,13,0.0666146514]";
    byte[] content = FileUtils.readFileToByteArray(ResourceUtils.getFile("classpath:data/targets.avro"));

    String schemaStr = "{\"type\": \"record\", \"version\": \"weitargets_1582801413158\", \"name\": \"weitargets\", \"namespace\": \"load.rdm\", \"fields\": [{\"type\": [\"int\"], \"name\": \"weiconfig_id\"}, {\"type\": [\"int\"], \"name\": \"property_id\"}, {\"type\": [\"int\"], \"name\": \"option_id\"}, {\"type\": [{\"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 10, \"type\": \"bytes\"}], \"name\": \"trg\"}]}";

    List<Row> rows = avroConverter.decode(schemaStr, content);

    assertEquals(310, rows.size());
    assertEquals(avroRecord, rows.get(0).toString());
  }

  @TestConfiguration
  static class AvroConverterImplTestContextConfiguration {

    @Bean
    @Primary
    public AvroConverter avroConverterService() {
      return new AvroConverter();
    }

    @PostConstruct
    public void init() {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
  }
}
