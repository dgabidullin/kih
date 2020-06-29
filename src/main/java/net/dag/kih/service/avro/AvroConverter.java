package net.dag.kih.service.avro;

import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;
import net.dag.kih.exception.AvroDecodeSchemaException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static java.util.Optional.ofNullable;

@Slf4j
@Service
public class AvroConverter implements Serializable {

  private static final long serialVersionUID = 1L;

  public StructType convertToStructType(String schemaAsStr) {
    Schema schema = new Schema.Parser().parse(schemaAsStr);
    return (StructType) SchemaConverters.toSqlType(schema).dataType();
  }

  public List<Row> decode(String schemaStr, byte[] val) {
    List<Row> result = new ArrayList<>();
    Schema schema = new Schema.Parser().parse(schemaStr);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

    SeekableByteArrayInput inputStream = new SeekableByteArrayInput(val);
    DataFileReader<GenericRecord> genericRecords;
    try {
      genericRecords = new DataFileReader<>(inputStream, datumReader);
    } catch (Exception e) {
      throw new AvroDecodeSchemaException("can't decode: " + e.getMessage());
    }

    StructType structType = convertToStructType(schemaStr);

    while (genericRecords.hasNext()) {
      GenericRecord record = genericRecords.next();

      Object[] objectArray = new Object[schema.getFields().size()];
      for (Schema.Field field : schema.getFields()) {

        Object obj = record.get(field.pos());
        if (obj == null)
          continue;
        LogicalType logicalType = field.schema().getType() != Schema.Type.UNION
            ? ofNullable(field.schema().getLogicalType()).orElse(null)
            : field.schema().getTypes().stream()
            .filter(x -> x.getLogicalType() != null)
            .findFirst()
            .map(Schema::getLogicalType)
            .orElse(null);
        String logicalTypeName = logicalType == null ? null : logicalType.getName();
        if (obj.getClass() == Utf8.class) {
          objectArray[field.pos()] = ((Utf8) obj).toString();
        } else if ("timestamp-millis".equals(logicalTypeName)) {
          objectArray[field.pos()] = new Timestamp((Long) record.get(field.pos()));
        } else if ("date".equals(logicalTypeName)) {
          objectArray[field.pos()] = Date.valueOf(LocalDate.ofEpochDay((Integer) record.get(field.pos())));
        } else if ("decimal".equals(logicalTypeName) && record.get(field.pos()) instanceof ByteBuffer) {
          ByteBuffer buf = (ByteBuffer) record.get(field.pos());
          byte[] arr = new byte[buf.remaining()];
          buf.get(arr);
          BigInteger bi = new BigInteger(1, arr);
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
          BigDecimal bd = new BigDecimal(bi).movePointLeft(decimal.getScale());
          objectArray[field.pos()] = bd;
        } else {
          objectArray[field.pos()] = record.get(field.pos());
        }
      }
      Row row = new GenericRowWithSchema(objectArray, structType);
      result.add(row);
    }

    return result;
  }
}
