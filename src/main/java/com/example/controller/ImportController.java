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
     * @param size
     * @return
     */
    @GetMapping("csv")
    public String csvImport(@RequestParam(required = true, defaultValue = "") String collectionName,
                            @RequestParam(required = true, defaultValue = "") String filepath,
                            @RequestParam(required = true, defaultValue = "") String target,
                            @RequestParam(required = true, defaultValue = "") int theadNum,
                            @RequestParam(required = true, defaultValue = "") int zoneMin,
                            @RequestParam(required = true, defaultValue = "") int zoneMax,
                            @RequestParam(required = true, defaultValue = "") int size) {
        return importService.importsDataOpt(collectionName, filepath, target,zoneMin,zoneMax, size, theadNum);
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


    @GetMapping("yibaomingxi")
    private String WideTable(@RequestParam(required = true, defaultValue = "") String collectionName,
                             @RequestParam(required = true, defaultValue = "") int theadNum,
                             @RequestParam(required = true, defaultValue = "") int dataSize){
        return importService.wideControl(collectionName,theadNum,dataSize);
    }

}

