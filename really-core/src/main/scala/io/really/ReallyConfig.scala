/**
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */
package io.really

import com.typesafe.config.{ Config, ConfigFactory }
import _root_.io.really.gorilla.{ GorillaConfig, EventLogStorageConfig }
import _root_.io.really.model.{ MongodbConfig, CollectionActorConfig, ShardingConfig }
import _root_.io.really.quickSand.QuickSandConfig

class ReallyConfig(config: Config) extends QuickSandConfig with ShardingConfig with CollectionActorConfig
    with MongodbConfig with EventLogStorageConfig with RequestDelegateConfig with GorillaConfig {
  protected val reference = ConfigFactory.defaultReference()

  protected val reallyConfig = config.getConfig("really") withFallback (reference.getConfig("really"))
  // validate against the reference configuration
  reallyConfig.checkValid(reference, "core")

  val coreConfig = reallyConfig.getConfig("core")
  def getRawConfig: Config = config
}
