package com.example.controller;

import com.example.service.ImportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.*;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.lang.Math.floor;

@RequestMapping("import")
@RestController
public class ImportController {
    //多线程
    @Autowired
    private ImportService importService;

    /**
     * 原始三张表
     *
     * @param collectionName 在mongodb中的表全称 例：com.ns.entity.object.form.instance.ns3g475kl6jj2eb4ixfi.yibaozhenduanxinxi_1
     * @param filepath       csv文件路径
     */
    @GetMapping("zhenduan")
    public String zhenduan(@RequestParam(required = true, defaultValue = "") String collectionName,
                                 @RequestParam(required = true, defaultValue = "") String filepath,
                                 @RequestParam(required = true, defaultValue = "") int theadNum,
                                 @RequestParam(required = true, defaultValue = "") int size) {
        return importService.importsDataOpt(collectionName, filepath,size,theadNum);
    }

    @GetMapping("gapdatagovern")
    private String GapDataGovernance(@RequestParam(required = true, defaultValue = "") String target,
                                     @RequestParam(required = true, defaultValue = "") String collectionName,
                                     @RequestParam(required = true, defaultValue = "") int theadnum
    ) {
        return importService.gapControl(target, collectionName, theadnum);
    }

    @GetMapping("datagovern")
    private String ZDataGovernance(@RequestParam(required = true, defaultValue = "") String target,
                                   @RequestParam(required = true, defaultValue = "") String collectionName,
                                   @RequestParam(required = true, defaultValue = "") int theadnum
    ) {
        return importService.statisticsControl(target, collectionName, theadnum);
    }
}

