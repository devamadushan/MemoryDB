package com.ARYD.MemoryDB.controller;

import com.ARYD.MemoryDB.entity.DataFrame;
import com.ARYD.MemoryDB.service.ParquetService;
import com.ARYD.MemoryDB.service.TableService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/tables")
@RequiredArgsConstructor
public class TablesController {
    private final ParquetService parquetService;
    private final TableService tableService; // Utilisation de TableService

    @PostMapping("/{name}")
    public DataFrame createTable(@PathVariable String name) {
        DataFrame df = new DataFrame();
        df.setTableName(name); // Associer le nom de la table
        parquetService.readParquetFile("src/data/test.parquet", df);
        tableService.addTable(df); // Ajouter dans TableService
        df.printAsCSV();
        return df; // Retourne le DataFrame en JSON
    }

    @GetMapping(value = "/{name}/csv", produces = "text/csv")
    public String getTableAsCSV(@PathVariable String name) {
        DataFrame df = tableService.getTableByName(name);
        return (df != null) ? df.toCSV() : "Table not found";
    }

    @GetMapping("/{name}/count")
    public int getTableRowCount(@PathVariable String name) {
        DataFrame df = tableService.getTableByName(name);
        return (df != null) ? df.countRows() : 0;
    }
}