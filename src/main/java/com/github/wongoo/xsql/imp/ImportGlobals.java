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

package com.github.wongoo.xsql.imp;

/**
 * @author Geln Yang
 * @version 1.0
 */
public final class ImportGlobals {

  private static boolean continueWhenError = false;

  private static boolean continueWhenPkFkError = false;

  private static int batchsize = 2000;

  private static int maxBulksize = 500000;

  private static boolean autoCommit = false;

  private static boolean validFirst = false;

  private static boolean changeNegativeToZero = false;

  public static boolean isContinueWhenError() {
    return continueWhenError;
  }

  public static void setContinueWhenError(boolean continueWhenError) {
    ImportGlobals.continueWhenError = continueWhenError;
  }

  public static boolean isContinueWhenPkFkError() {
    return continueWhenPkFkError;
  }

  public static void setContinueWhenPkFkError(boolean continueWhenPkFkError) {
    ImportGlobals.continueWhenPkFkError = continueWhenPkFkError;
  }

  public static int getBatchsize() {
    return batchsize;
  }

  public static void setBatchsize(int batchsize) {
    ImportGlobals.batchsize = batchsize;
  }

  public static boolean isAutoCommit() {
    return autoCommit;
  }

  public static void setAutoCommit(boolean autoCommit) {
    ImportGlobals.autoCommit = autoCommit;
  }

  public static boolean isValidFirst() {
    return validFirst;
  }

  public static void setValidFirst(boolean validFirst) {
    ImportGlobals.validFirst = validFirst;
  }

  public static int getMaxBulksize() {
    return maxBulksize;
  }

  public static void setMaxBulksize(int maxBulksize) {
    ImportGlobals.maxBulksize = maxBulksize;
  }

  public static boolean isChangeNegativeToZero() {
    return changeNegativeToZero;
  }

  public static void setChangeNegativeToZero(boolean changeNegativeToZero) {
    ImportGlobals.changeNegativeToZero = changeNegativeToZero;
  }

}
