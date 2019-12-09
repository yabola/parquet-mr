/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class ParquetMetrics {
  public static final Logger logger = LoggerFactory.getLogger(ParquetMetrics.class);

  private static ThreadLocal<ParquetMetrics> metric = new ThreadLocal<>();

  //  common
  private long pageReadHeaderTime;
  private long pageReadHeaderTmp;
  private long pageReadHeaderCnt;

  private long footerReadTime;
  private long footerReadTmp;
  private long footerReadCnt;

  private long groupReadTmp;
  private long groupReadTime;
  private long groupReadCnt;

  private long pageReadTime;
  private long pageReadTmp;
  private long pageReadCnt;

  private long filteredGroupReadTmp;
  private long filteredGroupReadTime;
  private long filteredGroupReadCnt;

  private long pageReadUncompressBytes;
  private long pageReadDecompressBytes;
  private long pageReadDecompressTime;
  private long pageReadDecompressTmp;

  private long totalPages;

  // parquet page index.
  private long colPageIndexReadCnt;
  private long colPageIndexReadTime;
  private long colPageIndexReadTmp;

  private long offsetPageIndexReadCnt;
  private long offsetPageIndexReadTime;
  private long offsetPageIndexReadTmp;

  public static ParquetMetrics get() {
    ParquetMetrics metrics = metric.get();
    if (metrics == null) {
      ParquetMetrics parquetMetrics = new ParquetMetrics();
      metric.set(parquetMetrics);
      return parquetMetrics;
    }
    return metrics;
  }

  public void groupReadStart() {
    groupReadTmp = System.nanoTime();
  }

  public void groupReadEnd() {
    groupReadCnt++;
    groupReadTime += System.nanoTime() - groupReadTmp;
  }

  public void filteredGroupReadStart() {
    filteredGroupReadTmp = System.nanoTime();
  }

  public void filteredGroupReadEnd() {
    filteredGroupReadCnt++;
    filteredGroupReadTime += System.nanoTime() - filteredGroupReadTmp;
  }

  public void pageReadStart() {
    pageReadTmp = System.nanoTime();
  }

  public void pageReadEnd() {
    pageReadCnt++;
    pageReadTime += System.nanoTime() - pageReadTmp;
  }

  public void columnIndexReadStart() {
    colPageIndexReadTmp = System.nanoTime();
  }

  public void columnIndexReadEnd() {
    colPageIndexReadCnt++;
    colPageIndexReadTime += System.nanoTime() - colPageIndexReadTmp;
  }

  public void offsetIndexReadStart() {
    offsetPageIndexReadTmp = System.nanoTime();
  }

  public void offsetIndexReadEnd() {
    offsetPageIndexReadCnt++;
    offsetPageIndexReadTime += System.nanoTime() - offsetPageIndexReadTmp;
  }

  public void footerReadStart() {
    footerReadTmp = System.nanoTime();
  }

  public void footerReadEnd() {
    footerReadCnt++;
    footerReadTime += System.nanoTime() - footerReadTmp;
  }

  public void pageReadHeaderStart() {
    pageReadHeaderTmp = System.nanoTime();
  }

  public void pageReadHeaderEnd() {
    pageReadHeaderCnt++;
    pageReadHeaderTime += System.nanoTime() - pageReadHeaderTmp;
  }

  public void pageReadDecompressStart() {
    pageReadDecompressTmp = System.nanoTime();
  }

  public void pageReadDecompressEnd(long uncompressedBytes, long decompressedBytes) {
    pageReadUncompressBytes += uncompressedBytes;
    pageReadDecompressBytes += decompressedBytes;
    pageReadDecompressTime += System.nanoTime() - pageReadDecompressTmp;
  }

  public void accumulateTotalPages(int totalPages) {
    this.totalPages += totalPages;
  }

  public void reset() {
    metric.set(null);
  }

  public String summary() {
    StringBuilder sb = new StringBuilder();
    Map<String, Long> metric = getMetric();
    sb.append("PARQUET METRICS: \n");
    for (String k : metric.keySet()) {
      sb.append("...").append(k).append("\t").append(metric.get(k)).append("\n");
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    Map<String, Long> metric = getMetric();
    StringBuilder sb = new StringBuilder();
    for (String k : metric.keySet()) {
      Long v = metric.get(k);
      if (v != 0) {
        sb.append(k).append("\t").append(v).append("| ");
      }
    }
    return sb.toString();
  }

  public Map<String, Long> getMetric() {
    Map<String, Long> metric = new LinkedHashMap<>();
    // common
    metric.put("pageReadHeaderTime", pageReadHeaderTime / 1000000);
    metric.put("pageReadHeaderCnt", pageReadHeaderCnt);

    metric.put("footerReadTime", footerReadTime / 1000000);
    metric.put("footerReadCnt", footerReadCnt);

    metric.put("groupReadTime", groupReadTime / 1000000);
    metric.put("groupReadCnt", groupReadCnt);

    metric.put("pageReadTime", pageReadTime / 1000000);
    metric.put("pageReadCnt", pageReadCnt);

    metric.put("filteredGroupReadTime", filteredGroupReadTime / 1000000);
    metric.put("filteredGroupReadCnt", filteredGroupReadCnt);

    metric.put("pageReadDecompressTime", pageReadDecompressTime / 1000000);
    metric.put("pageReadUncompressBytes", pageReadUncompressBytes);
    metric.put("pageReadDecompressBytes", pageReadDecompressBytes);

    if (totalPages != 0) {
      metric.put("totalPages", totalPages);
      metric.put("filterPagesRatio", pageReadCnt/totalPages);
    }

    metric.put("colPageIndexReadTime", colPageIndexReadTime / 1000000);
    metric.put("colPageIndexReadCnt", colPageIndexReadCnt);

    metric.put("offsetPageIndexReadTime", offsetPageIndexReadTime / 1000000);
    metric.put("offsetPageIndexReadCnt", offsetPageIndexReadCnt);

    metric.put("totalTime", getTotalTime());
    return metric;
  }

  public long getTotalTime() {
    return (
      pageReadHeaderTime +
        footerReadTime +
        groupReadTime +
        pageReadTime +
        filteredGroupReadTime +
        pageReadDecompressTime +
        colPageIndexReadTime +
        offsetPageIndexReadTime) / 1000000;
  }

  public long getPageReadHeaderTime() {
    return pageReadHeaderTime / 1000000;
  }

  public long getPageReadHeaderCnt() {
    return pageReadHeaderCnt;
  }

  public long getFooterReadTime() {
    return footerReadTime / 1000000;
  }

  public long getFooterReadCnt() {
    return footerReadCnt;
  }

  public long getGroupReadTime() {
    return groupReadTime / 1000000;
  }

  public long getGroupReadCnt() {
    return groupReadCnt;
  }

  public long getPageReadTime() {
    return pageReadTime / 1000000;
  }

  public long getPageReadCnt() {
    return pageReadCnt;
  }

  public long getFilteredGroupReadTime() {
    return filteredGroupReadTime / 1000000;
  }

  public long getFilteredGroupReadCnt() {
    return filteredGroupReadCnt;
  }

  public long getPageReadUncompressBytes() {
    return pageReadUncompressBytes;
  }

  public long getPageReadDecompressBytes() {
    return pageReadDecompressBytes;
  }

  public long getPageReadDecompressTime() {
    return pageReadDecompressTime / 1000000;
  }

  public long getTotalPages() {
    return totalPages;
  }

  public long getColPageIndexReadCnt() {
    return colPageIndexReadCnt;
  }

  public long getColPageIndexReadTime() {
    return colPageIndexReadTime / 1000000;
  }

  public long getOffsetPageIndexReadCnt() {
    return offsetPageIndexReadCnt;
  }

  public long getOffsetPageIndexReadTime() {
    return offsetPageIndexReadTime / 1000000;
  }
}
