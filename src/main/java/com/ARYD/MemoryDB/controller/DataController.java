package com.ARYD.MemoryDB.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/data")
public class DataController {

    @PostMapping("/{tableName}")
    public ResponseEntity<Void> insertData(@PathVariable String tableName, @RequestBody Map<String, Object> row) {
        return null;
    }

    @GetMapping("/{tableName}")
    public ResponseEntity<List<Map<String, Object>>> getAllData(@PathVariable String tableName) {
        return null;
    }

}
