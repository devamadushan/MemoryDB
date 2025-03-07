package com.ARYD.MemoryDB.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Table {
    private String name;
    private List<Column> columns;
    private List<Map<String, Object>> rows = new ArrayList<>();
}
