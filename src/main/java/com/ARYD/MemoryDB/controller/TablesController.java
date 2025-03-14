package com.ARYD.MemoryDB.controller;

import com.ARYD.MemoryDB.service.DataFrameService;
import com.ARYD.MemoryDB.service.ParquetService;
import com.ARYD.MemoryDB.service.TablesService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/tables")
@RequiredArgsConstructor
public class TablesController {
    private final ParquetService parquetService;
    private final TablesService tablesService; // Utilisation de TableService

    @PostMapping("/{name}")
    public String createTable(@PathVariable String name , @RequestBody Map<String, String> request) {
        String filePath = request.get("filePath");

        DataFrameService df = new DataFrameService();
        df.setTableName(name); // Associer le nom de la table
        parquetService.readParquetFile(filePath, df);
        tablesService.addTable(df); // Ajouter dans TableService
        df.printAsCSV();
        return " Table a éte inseret correctement"; //
    }

    @GetMapping(value = "/{name}/csv", produces = "text/csv")
    public String getTableAsCSV(@PathVariable String name) {
        DataFrameService df = tablesService.getTableByName(name);
        return (df != null) ? df.toCSV() : "Table not found";
    }

    @GetMapping("/{name}/count")
    public int getTableRowCount(@PathVariable String name) {
        DataFrameService df = tablesService.getTableByName(name);
        return (df != null) ? df.countRows() : 0;
    }
}