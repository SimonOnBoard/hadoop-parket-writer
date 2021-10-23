package com.simononboard.hadoopwriter.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Component
public class ExampleParquetWriter {

    @PostConstruct
    public void init() {
        System.out.println("Started parket worker");
        Schema schema = parseSchema();
        System.out.println("Schema loaded");
        List<GenericData.Record> recordList = createRecords(schema);
        writeToParquetFile(recordList, schema);
    }

    // Method to parse the schema
    private Schema parseSchema() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            // Path to schema file
            schema = parser.parse(ResourceUtils.getFile("classpath:schema.avsc"));
        } catch (IOException e) {
            throw new IllegalStateException("schema reading failed");
        }
        return schema;
    }

    private  List<GenericData.Record> createRecords(Schema schema) {
        List<GenericData.Record> recordList = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("id", i);
            record.put("empName", i + "r213fndsfnerq");
            recordList.add(record);
        }
        return recordList;
    }

    private  void writeToParquetFile(List<GenericData.Record> recordList, Schema schema) {
        // Output path for Parquet file in HDFS
        Path path = new Path("/user/parket/data.parquet/1232.parquet");
        ParquetWriter<GenericData.Record> writer = null;
        // Creating ParquetWriter using builder
        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();
            // writing records
            for (GenericData.Record record : recordList) {
                writer.write(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
}