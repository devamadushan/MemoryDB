package com.ARYD.MemoryDB.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/query")
public class QueryController {

    @GetMapping("/{tableName}/where")
    public ResponseEntity<List<Map<String, Object>>> filterData(
            @PathVariable String tableName,
            @RequestParam String column,
            @RequestParam Object value) {
        return null;
    }

    @GetMapping("/{tableName}/groupby")
    public ResponseEntity<Map<Object, List<Map<String, Object>>>> groupBy(
            @PathVariable String tableName,
            @RequestParam String column) {
        return null;
    }

    @GetMapping("/{tableName}/orderby")
    public ResponseEntity<List<Map<String, Object>>> orderBy(
            @PathVariable String tableName,
            @RequestParam String column,
            @RequestParam(defaultValue = "asc") String order) {
        return null;
    }
}
