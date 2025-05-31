package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;
import com.udacity.webcrawler.profiler.Profiled;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final PageParserFactory parserFactory;
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final ForkJoinPool pool;

  @Inject
  ParallelWebCrawler(PageParserFactory parserFactory,
                     Clock clock,
                     @Timeout Duration timeout,
                     @PopularWordCount int popularWordCount,
                     @TargetParallelism int threadCount,
                     @MaxDepth int maxDepth,
                     @IgnoredUrls List<Pattern> ignoredUrls) {
    this.parserFactory = parserFactory;
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {

    ConcurrentMap<String, Integer> wordCounts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
    Instant countdown = clock.instant().plus(timeout);

    startingUrls.forEach(startingUrl -> pool.invoke(new CrawlerTask(startingUrl, maxDepth, wordCounts, visitedUrls,
            countdown, ignoredUrls)));

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(wordCounts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

  private class CrawlerTask extends RecursiveTask<Boolean> {
    private String url;
    private int maxDepth;
    private ConcurrentMap<String, Integer> wordCounts;
    private ConcurrentSkipListSet<String> visitedUrls;
    private Instant countdown;
    private List<Pattern> ignoredUrls;

    public CrawlerTask(String url, int maxDepth, ConcurrentMap<String, Integer> wordCounts,
                       ConcurrentSkipListSet<String> visitedUrls, Instant countdown, List<Pattern> ignoredUrls) {
      this.url = url;
      this.maxDepth = maxDepth;
      this.wordCounts = wordCounts;
      this.visitedUrls = visitedUrls;
      this.countdown = countdown;
      this.ignoredUrls = ignoredUrls;
    }

    @Override
    protected Boolean compute() {
      if (maxDepth == 0) {
        return false;
      }
      if (visitedUrls.contains(url)) {
        return false;
      }
      if (clock.instant().isAfter(countdown)) {
        return false;
      }
      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          return false;
        }
      }

      PageParser.Result result = parserFactory.get(url).parse();

      for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        wordCounts.compute(e.getKey(), (k, v) -> (v == null) ? e.getValue() : e.getValue() + v);
      }

      visitedUrls.add(url);

      List<CrawlerTask> subtasks = new ArrayList<>();
      for (String link : result.getLinks()) {
        subtasks.add(new CrawlerTask(link, maxDepth - 1, wordCounts, visitedUrls, countdown, ignoredUrls));
      }
      invokeAll(subtasks);

      return true;
    }
  }

    @Override
    public int getMaxParallelism() {
      return Runtime.getRuntime().availableProcessors();
    }

  }