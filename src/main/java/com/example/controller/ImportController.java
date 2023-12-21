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
     * @param collectionName 入库名
     * @param filepath       文件位置
     * @param target         写入目标
     * @param theadNum       线程数
     * @param zoneMin        zone最小范围
     * @param zoneMax        zone最大范围
     * @param columnSize     列数
     * @param rowsSize       总行数
     * @return 调用结果
     */
    @PostMapping("csv")
    public String csvImport(@RequestParam(defaultValue = "") String collectionName,
                            @RequestParam(defaultValue = "") String filepath,
                            @RequestParam(defaultValue = "") String target,
                            @RequestParam(defaultValue = "") int theadNum,
                            @RequestParam(defaultValue = "") int zoneMin,
                            @RequestParam(defaultValue = "") int zoneMax,
                            @RequestParam(defaultValue = "") int columnSize,
                            @RequestParam(defaultValue = "") long rowsSize) {
        return importService.importsDataOpt(collectionName, filepath, target, zoneMin, zoneMax, columnSize, rowsSize, theadNum);
    }

    /**
     * 大宽表导入
     *
     * @param collectionName 入库名
     * @param theadNum       开启线程数量
     * @param dataSize       分到此台机器的数据量
     * @return 调用结果
     */

    @GetMapping("wide")
    private String WideTable(@RequestParam(defaultValue = "") String collectionName,
                             @RequestParam(defaultValue = "") int theadNum,
                             @RequestParam(defaultValue = "") int dataSize) {
        return importService.wideControl(collectionName, theadNum, dataSize);
    }


}

