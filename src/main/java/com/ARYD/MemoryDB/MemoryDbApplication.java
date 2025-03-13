package com.ARYD.MemoryDB;
import com.ARYD.MemoryDB.service.TableService;
import com.ARYD.MemoryDB.service.ParquetService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MemoryDbApplication {

	public static void main(String[] args) {

		SpringApplication.run(MemoryDbApplication.class, args);

}
}
