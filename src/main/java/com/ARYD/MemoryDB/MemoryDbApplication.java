package com.ARYD.MemoryDB;
import com.ARYD.MemoryDB.entity.Column;
import com.ARYD.MemoryDB.service.TableService;
import org.springframework.context.ApplicationContext;
import com.ARYD.MemoryDB.service.ParquetService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.print.DocFlavor;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class MemoryDbApplication {

	public static void main(String[] args) {

		SpringApplication.run(MemoryDbApplication.class, args);

		///  Exemple d'insertion

		String path_parquet = "src/data/test.parquet";
		String name_parquet = "test";


		TableService tableService = new TableService();
		ParquetService parquetService = new ParquetService(tableService);

		parquetService.insertParquetIntoTable(path_parquet, name_parquet);

		List<Map<String, Object>> rows = tableService.getTables().get(0).getRows();
		System.out.println("Nombre total de lignes insérées : " + rows.size());

		for (int i = 0; i < Math.min(10, rows.size()); i++) {
			System.out.println(rows.get(i));

		}

}
}
