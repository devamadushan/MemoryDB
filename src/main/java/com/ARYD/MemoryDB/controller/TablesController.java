package com.ARYD.MemoryDB.controller;

import com.ARYD.MemoryDB.entity.Column;
import com.ARYD.MemoryDB.entity.Table;
import com.ARYD.MemoryDB.service.TableService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/tables")
@RequiredArgsConstructor
public class TablesController {
    private final TableService tablesService;

    @GetMapping
    public List<Table> getTables() {
        return tablesService.getTables();
    }

    @PostMapping
    public Table createTable(String name, List<Column> columns) {
        return tablesService.createTable(name, columns);
    }

    @DeleteMapping("/{name}")
    public void deleteTable(@PathVariable String name) {
        tablesService.deleteTable(name);
    }

    @PostMapping("/{name}/addRow")
    public Table insertRow(@PathVariable String name, @RequestBody Map<String,Object> rows) {
        return tablesService.insertRow(name, rows);
    }

    @PatchMapping("/{name}")
    public Table updateTable(@PathVariable String name, @RequestBody List<Column> columns) {
        return tablesService.updateTable(name, columns);
    }

    @GetMapping("/Hello")
    public String Hello(){
        return "Hello RAQYD";
    }


}
