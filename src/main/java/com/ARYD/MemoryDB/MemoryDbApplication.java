package com.ARYD.MemoryDB;

import com.ARYD.MemoryDB.service.ParquetService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.print.DocFlavor;

@SpringBootApplication
public class MemoryDbApplication {

	public static void main(String[] args) {

		//SpringApplication.run(MemoryDbApplication.class, args);
		String path_parquet = "src/data/test.parquet";
		ParquetService parquetReaderService_test = new ParquetService();
		parquetReaderService_test.readParquetFile(path_parquet);
	}

}
