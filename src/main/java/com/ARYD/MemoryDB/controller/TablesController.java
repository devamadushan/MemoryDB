package com.ARYD.MemoryDB.controller;

import com.ARYD.MemoryDB.entity.Column;
import com.ARYD.MemoryDB.entity.DataFrame;
import com.ARYD.MemoryDB.entity.Table;
import com.ARYD.MemoryDB.service.ParquetService;
import com.ARYD.MemoryDB.service.TableService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/tables")
@RequiredArgsConstructor
public class TablesController {
    private final ParquetService parquetService;
    private final List<DataFrame> tables = new ArrayList<>();

    @PostMapping("/{name}")
    public DataFrame createTable(@PathVariable String name) {
        DataFrame df = new DataFrame();
        df.setTableName(name); // Associer le nom de la table
        parquetService.readParquetFile("src/data/test.parquet", df);
        tables.add(df);
        return df; // Spring Boot va convertir cet objet en JSON
    }

    @GetMapping("/{name}")
    public DataFrame getTable(@PathVariable String name) {
        return tables.stream()
                .filter(df -> name.equals(df.getTableName()))
                .findFirst()
                .orElse(null);
    }
}
