package com.jay.avro.generic;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericRecordExamples {

	public static void main(String[] args) {
		
		// define schema
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse("{\r\n" + 
				"     \"type\": \"record\",\r\n" + 
				"     \"namespace\": \"com.example\",\r\n" + 
				"     \"name\": \"Customer\",\r\n" + 
				"     \"fields\": [\r\n" + 
				"       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\r\n" + 
				"       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\r\n" + 
				"       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\r\n" + 
				"       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\r\n" + 
				"       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\r\n" + 
				"       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\r\n" + 
				"     ]\r\n" + 
				"}");
		
		// create generic record
		GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
		customerBuilder.set("first_name", "John");
		customerBuilder.set("last_name", "Doe");
		customerBuilder.set("age", 23);
		customerBuilder.set("height", 178f);
		customerBuilder.set("weight", 80.5f);
		customerBuilder.set("automated_email", false);
		GenericData.Record customer = customerBuilder.build();
		System.out.println(customer);
		
		// write that generic record to a file
		final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        try {
        	DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(customer);
            dataFileWriter.close();
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
                
		// read the generic record from file
        final File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        GenericRecord customerRead;
        try {
        	DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        	
        	// interpret as general record
        	customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());
        	

            // get the data from the generic record
            System.out.println("First name: " + customerRead.get("first_name"));
            // read a non existent field -> returns null
            System.out.println("Non existent field: " + customerRead.get("not_here"));

            dataFileReader.close();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
	}
}
