// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{ BeforeAndAfterAll, Suite }
import com.typesafe.config.Config

// Provides and manages an ActorSystem for tests that don't extend ScalatestRouteTest (which provides an ActorSystem).
trait ActorSystemTestKit extends BeforeAndAfterAll { self: Suite =>

  // Only need to override this if you want an ActorSystem with a special config. For example, tied to
  // a certain S3 container. See S3StreamClientSpec
  var testConfig: Option[Config] = None

  // lazy so that if optional testConfig is used, it can be initialized in an afterStart.
  implicit lazy val system: ActorSystem = {
    val name = s"test-system-${getClass.getSimpleName}"
    testConfig match {
      case Some(config) => ActorSystem(name, config)
      case None => ActorSystem(name)
    }
  }

  override def afterAll(): Unit = {
    try {
      TestKit.shutdownActorSystem(system)
    } finally {
      super.afterAll()
    }
  }

}
