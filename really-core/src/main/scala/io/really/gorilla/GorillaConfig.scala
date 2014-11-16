/*
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */
package io.really.gorilla

import java.util.concurrent.TimeUnit

import io.really.ReallyConfig

import scala.concurrent.duration._

trait GorillaConfig {
  this: ReallyConfig =>

  object GorillaConfig {
    protected val gorilla = coreConfig.getConfig("gorilla")
    val waitForSnapshot = gorilla.getDuration("wait-for-snapshot", TimeUnit.MILLISECONDS).milliseconds
    val waitForGorillaCenter = gorilla.getDuration("wait-for-gorilla_center", TimeUnit.MILLISECONDS).milliseconds
    val advancedRevisionLimit = gorilla.getLong("advanced-revision-diff")
    val waitForModel = gorilla.getDuration("wait-for-model", TimeUnit.MILLISECONDS).milliseconds
    //TODO    val idleTimeout = gorilla.getDuration("idle-timeout", TimeUnit.MILLISECONDS).milliseconds
  }

}
