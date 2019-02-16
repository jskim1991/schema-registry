package com.jay.avro.specific;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.jay.avro.Customer;

/**
 * Lecture 17: For some reason, Customer class is not found under target. So, I added
 * outputDirectory in pom and refresh the project to import. Also, generated
 * Customer's build() method shows override error.
 * 
 * @author guyko
 *
 */
public class SpecificRecordExamples {

	public static void main(String[] args) {

		// create a specific record
		Customer.Builder customerBuilder = Customer.newBuilder();
		customerBuilder.setAge(25);
		customerBuilder.setFirstName("John");
		customerBuilder.setLastName("Doe");
		customerBuilder.setHeight(178f);
		customerBuilder.setWeight(89.2f);

		Customer customer = customerBuilder.build();
		System.out.println(customer);

		// write to a file
		final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<Customer>(Customer.class);
		try {
			DataFileWriter<Customer> dataFileWriter = new DataFileWriter<Customer>(datumWriter);
			dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
			dataFileWriter.append(customer);
			dataFileWriter.close(); // need this or next() will throw error
			System.out.println("Written customer-specific.avro");
		} catch (IOException e) {
			System.out.println("Couldn't write file");
			e.printStackTrace();
		}
		// read from file
		final File file = new File("customer-specific.avro");
		final DatumReader<Customer> datumReader = new SpecificDatumReader<Customer>(Customer.class);
		final DataFileReader<Customer> dataFileReader;
		try {
			System.out.println("Reading our specific record");
			dataFileReader = new DataFileReader<Customer>(file, datumReader);
			while (dataFileReader.hasNext()) {
				// interpret
				Customer readCustomer = dataFileReader.next();
				System.out.println(readCustomer.toString());
				System.out.println("First name: " + readCustomer.getFirstName());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
