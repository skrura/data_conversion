package com.example.controller;

import com.example.service.ImportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RequestMapping("import")
@RestController
public class ImportController {
    //多线程
    @Autowired
    private ImportService importService;

    /**
     * csv导入原始三张表
     *
     * @param collectionName
     * @param filepath
     * @param target
     * @param theadNum
     * @param columnSize
     * @param rowsSize
     * @return
     */
    @GetMapping("csv")
    public String csvImport(@RequestParam(required = true, defaultValue = "") String collectionName,
                            @RequestParam(required = true, defaultValue = "") String filepath,
                            @RequestParam(required = true, defaultValue = "") String target,
                            @RequestParam(required = true, defaultValue = "") int theadNum,
                            @RequestParam(required = true, defaultValue = "") int zoneMin,
                            @RequestParam(required = true, defaultValue = "") int zoneMax,
                            @RequestParam(required = true, defaultValue = "") int columnSize,
                            @RequestParam(required = true, defaultValue = "") long rowsSize) {
        return importService.importsDataOpt(collectionName, filepath, target, zoneMin, zoneMax, columnSize, rowsSize, theadNum);
    }


    @GetMapping("yibaomingxi")
    private String WideTable(@RequestParam(required = true, defaultValue = "") String collectionName,
                             @RequestParam(required = true, defaultValue = "") int theadNum,
                             @RequestParam(required = true, defaultValue = "") int dataSize) {
        return importService.wideControl(collectionName, theadNum, dataSize);
    }


}

