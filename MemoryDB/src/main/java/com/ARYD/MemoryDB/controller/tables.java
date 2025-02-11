package com.ARYD.MemoryDB.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/tables")
public class tables {

    @GetMapping("/Hello")
    public String Hello(){
        return "Hello RAQYD";
    }


}
