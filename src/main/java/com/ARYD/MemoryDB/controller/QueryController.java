package com.ARYD.MemoryDB.controller;

import com.ARYD.MemoryDB.service.QueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/query")
@RequiredArgsConstructor
public class QueryController {
    private final QueryService queryService;

    @PostMapping
    public Object executeQuery(@RequestBody String query) {
        return queryService.executeQuery(query);
    }
}