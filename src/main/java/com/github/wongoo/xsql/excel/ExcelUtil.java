/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.wongoo.xsql.excel;

import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Geln Yang
 * @version 1.0
 */
@Slf4j
public class ExcelUtil {

    /**
     * @param file
     * @return
     * @throws IOException
     * @throws
     * @throws
     */
    public static XSSFWorkbook readExcel(File file) throws IOException {
        InputStream inp = new FileInputStream(file);
        // If clearly doesn't do mark/reset, wrap up
        if (!inp.markSupported()) {
            inp = new PushbackInputStream(inp, 8);
        }
        try {
            return new XSSFWorkbook(OPCPackage.open(inp));
        } catch (InvalidFormatException e) {
            return new XSSFWorkbook(new FileInputStream(file));
        }
    }

    public static HSSFWorkbook readExcel2003(File file) throws IOException {
        return new HSSFWorkbook(new FileInputStream(file));
    }

    /**
     * 讀取excel檔案內容
     */
    public static List<List<Object>> readExcelLines(XSSFWorkbook workbook, int sheetIndex, int fromRowIndex,
        int toRowIndex) throws IOException {
        if (fromRowIndex > toRowIndex && toRowIndex >= 0) {
            throw new IOException("The fromRowIndex is great than toRowIndex!");
        }
        XSSFSheet sheet = workbook.getSheetAt(sheetIndex);
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        if (null != sheet) {
            int maxRowNum = sheet.getLastRowNum();
            if (toRowIndex < 0) {
                toRowIndex = maxRowNum;
            }
            if (toRowIndex < 0 || maxRowNum < toRowIndex) {
                toRowIndex = maxRowNum;
                // throw new
                // IOException("The toRowIndex is great than the max row num " +
                // maxRowNum);
            }
            List<List<Object>> lines = new ArrayList<List<Object>>();
            for (int rowIndex = fromRowIndex; rowIndex <= toRowIndex; rowIndex++) {
                if (null != sheet.getRow(rowIndex)) {
                    XSSFRow row = sheet.getRow(rowIndex);
                    int cellCount = row.getLastCellNum();
                    List<Object> line = new ArrayList<Object>();
                    lines.add(line);
                    for (int cellIndex = 0; cellIndex < cellCount; cellIndex++) {
                        XSSFCell cell = row.getCell(cellIndex);
                        Object cellVal = "";
                        if (null != cell) {
                            cellVal = getCellValue(cell, evaluator);
                        }
                        line.add(cellVal);
                    }
                }
            }// END for

            return lines;
        } else {
            log.warn("no sheet index is " + sheetIndex);
        }
        log.warn("no sheet found!");
        return null;
    }

    /**
     * 讀取excel檔案內容
     */
    public static List<List<Object>> readExcelLines2003(HSSFWorkbook workbook, int sheetIndex, int fromRowIndex,
        int toRowIndex) throws IOException {
        if (fromRowIndex > toRowIndex && toRowIndex >= 0) {
            throw new IOException("The fromRowIndex is great than toRowIndex!");
        }
        HSSFSheet sheet = workbook.getSheetAt(sheetIndex);
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        if (null != sheet) {
            int maxRowNum = sheet.getLastRowNum();
            if (toRowIndex < 0) {
                toRowIndex = maxRowNum;
            }
            if (maxRowNum < toRowIndex) {
                toRowIndex = maxRowNum;
                // throw new
                // IOException("The toRowIndex is great than the max row num " +
                // maxRowNum);
            }
            List<List<Object>> lines = new ArrayList<List<Object>>();
            for (int rowIndex = fromRowIndex; rowIndex <= toRowIndex; rowIndex++) {
                if (null != sheet.getRow(rowIndex)) {
                    HSSFRow row = sheet.getRow(rowIndex);
                    int cellCount = row.getLastCellNum();
                    List<Object> line = new ArrayList<Object>();
                    lines.add(line);
                    for (int cellIndex = 0; cellIndex < cellCount; cellIndex++) {
                        HSSFCell cell = row.getCell(cellIndex);
                        Object cellVal = "";
                        if (null != cell) {
                            cellVal = getCellValue2003(cell, evaluator);
                        }
                        line.add(cellVal);
                    }
                }
            }// END for

            return lines;
        } else {
            log.warn("no sheet index is " + sheetIndex);
        }
        log.warn("no sheet found!");
        return null;
    }

    /**
     * @param cell
     * @param evaluator
     * @return
     */
    private static Object getCellValue(XSSFCell cell, FormulaEvaluator evaluator) {
        DataFormatter dataFormatter = new DataFormatter();
        CellType cellType = cell.getCellType();
        Object cellVal = null;
        switch (cellType) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    cellVal = cell.getDateCellValue();
                } else {
                    cellVal = cell.getNumericCellValue();
                }
                break;
            case STRING:
                cellVal = cell.getStringCellValue();
                break;
            case BOOLEAN:
                cellVal = cell.getBooleanCellValue();
                break;
            case FORMULA:
                try {
                    cellVal = dataFormatter.formatCellValue(cell, evaluator);
                } catch (Exception e) {
                    if (e.getMessage().indexOf("not implemented yet") != -1) {
                        log.warn(e.getMessage());
                    } else {
                        throw new RuntimeException(e);
                    }
                }

                break;
            default:
                cellVal = cell.getStringCellValue();
        }
        return cellVal;
    }

    /**
     * @param cell
     * @param evaluator
     * @return
     */
    private static Object getCellValue2003(HSSFCell cell, FormulaEvaluator evaluator) {
        DataFormatter dataFormatter = new DataFormatter();
        CellType cellType = cell.getCellType();
        Object cellVal = null;
        switch (cellType) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    cellVal = cell.getDateCellValue();
                } else {
                    cellVal = cell.getNumericCellValue();
                }
                break;
            case STRING:
                cellVal = cell.getStringCellValue();
                break;
            case BOOLEAN:
                cellVal = cell.getBooleanCellValue();
                break;
            case FORMULA:
                try {
                    cellVal = dataFormatter.formatCellValue(cell, evaluator);
                } catch (Exception e) {
                    if (e.getMessage().indexOf("Could not resolve external workbook name") != -1
                        || e.getMessage().indexOf("Unexpected celltype (5)") != -1) {
                        log.warn(e.getMessage());
                    } else {
                        throw new RuntimeException(e);
                    }
                }
                break;
            default:
                cellVal = cell.getStringCellValue();
        }
        return cellVal;
    }

    /**
     * 從ppt中賺取excel
     */
    @SuppressWarnings("resource")
    public static File fetchExcelFromPPT(File pptFile, int slideIndex, int shapeIndex) throws IOException {
        HSLFSlideShow ppt = new HSLFSlideShow(new FileInputStream(pptFile));

        List<HSLFSlide> slides = ppt.getSlides();
        log.debug("Slide count: " + slides.size());

        for (int i = 0; i < slides.size(); i++) {
            if (i != slideIndex) {
                continue;
            }
            HSLFSlide slide = slides.get(i);
            log.debug("Slide NO:" + slide.getSlideNumber());
            String excelTitle = slide.getTitle();
            excelTitle = excelTitle.replace("\r", "").replace("\n", "");
            log.debug(slide.getTitle());
            List<HSLFShape> shapes = slide.getShapes();
            log.debug("shapes length:" + shapes.size());

            for (int j = 0; j < shapes.size(); j++) {

                if (shapeIndex >= 0 && j != shapeIndex) {
                    continue;
                }

                HSLFShape shape = shapes.get(j);

                // TODO to find the excel file and write to file

                //                if (shape instanceof OLEShape) {
                //                    OLEShape oleShape = (OLEShape) shape;
                //                    log.debug(oleShape.getInstanceName());
                //                    log.debug(oleShape.getFullName());
                //
                //                    HSSFWorkbook wb = new HSSFWorkbook(oleShape.getObjectData().getData());
                //                    String excelFileName =
                //                            FilenameUtils.getBaseName(pptFile.getName()) + "_" + slideIndex + "_"
                //                                    + excelTitle + ".xls";
                //                    File excel = new File(pptFile.getParent() + File.separator + excelFileName);
                //                    log.debug("write file: " + excel.getAbsolutePath());
                //                    wb.write(new FileOutputStream(excel));
                //                    return excel;
                //                }
                if (shapeIndex >= 0) {
                    break; // END
                }
            }

            break;// END
        }
        log.warn("no excel found!");
        return null;

    }

    public static File fetchExcelFromPPT(File pptFile, int slideIndex) throws IOException {
        return fetchExcelFromPPT(pptFile, slideIndex, -1);
    }

    public static List<List<Object>> readExcelLines(File file, int sheetIndex, int fromRowIndex, int toRowIndex)
        throws IOException {
        Workbook excel;
        try {
            excel = readExcel2003(file);
        } catch (Exception e) {
            log.debug(e.getMessage());
            excel = readExcel(file);
        }
        if (excel instanceof XSSFWorkbook) {
            return readExcelLines((XSSFWorkbook)excel, sheetIndex, fromRowIndex, toRowIndex);
        } else if (excel instanceof HSSFWorkbook) {
            return readExcelLines2003((HSSFWorkbook)excel, sheetIndex, fromRowIndex, toRowIndex);
        }
        return null;
    }

    public static List<List<Object>> readExcelLines(File workbook, int sheetIndex, int fromRowIndex)
        throws IOException {
        return readExcelLines(workbook, sheetIndex, fromRowIndex, -1);
    }

    public static Workbook create(InputStream inp) throws IOException {
        try {
            // If clearly doesn't do mark/reset, wrap up
            if (!inp.markSupported()) {
                inp = new PushbackInputStream(inp, 8);
            }

            //TODO to fix
            //            if (POIFSFileSystem.hasPOIFSHeader(inp)) {
            //                return new HSSFWorkbook(inp);
            //            }
            //            if (POIXMLDocument.hasOOXMLHeader(inp)) {
            //                return new XSSFWorkbook(OPCPackage.open(inp));
            //            }

            throw new IllegalArgumentException("Your InputStream was neither an OLE2 stream, nor an OOXML stream");
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    public static int getNumberOfSheets(File file) throws IOException {
        Workbook excel;
        try {
            excel = readExcel2003(file);
        } catch (Exception e) {
            log.debug(e.getMessage());
            excel = readExcel(file);
        }
        return excel.getNumberOfSheets();

    }
}
