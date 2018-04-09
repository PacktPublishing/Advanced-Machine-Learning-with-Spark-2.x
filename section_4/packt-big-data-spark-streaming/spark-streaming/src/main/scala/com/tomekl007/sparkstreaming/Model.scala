package com.tomekl007.sparkstreaming

import java.time.{ZoneId, ZonedDateTime}

import com.tomekl007.WithUserId

case class PageView(pageViewId: Int, userId: String, url: String, eventTime: ZonedDateTime)

case class PageViewWithViewCounter(pageViewId: Int, userId: String, url: String,
                                   eventTime: ZonedDateTime, viewsCounter: Int) extends WithUserId

object PageViewWithViewCounter {
  def withVisitCount(event: PageView, visitCount: Int): PageViewWithViewCounter = {
    PageViewWithViewCounter(event.pageViewId, event.userId, event.url, event.eventTime.withZoneSameInstant(ZoneId.of("UTC")), visitCount)
  }
}