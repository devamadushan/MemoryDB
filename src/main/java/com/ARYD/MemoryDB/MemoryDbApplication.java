package com.ARYD.MemoryDB;

import com.ARYD.MemoryDB.service.TableService;
import com.ARYD.MemoryDB.service.ParquetService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;
import java.util.Map;

@SpringBootApplication
public class MemoryDbApplication {

	public static void main(String[] args) {
		// Lancer l'application et récupérer le contexte Spring
		ConfigurableApplicationContext context = SpringApplication.run(MemoryDbApplication.class, args);

		// Exemple d'insertion
		String path_parquet = "src/data/test.parquet";
		String name_parquet = "test";

		// Récupérer les beans gérés par Spring
		TableService tableService = context.getBean(TableService.class);
		ParquetService parquetService = context.getBean(ParquetService.class);

		// Insertion des données depuis le fichier Parquet dans la table
		parquetService.insertParquetIntoTable(path_parquet, name_parquet);

		// Affichage des données insérées
		List<Map<String, Object>> rows = tableService.getTables().get(0).getRows();
		System.out.println("Nombre total de lignes insérées : " + rows.size());
		for (int i = 0; i < Math.min(10, rows.size()); i++) {
			System.out.println(rows.get(i));
		}
	}
}
