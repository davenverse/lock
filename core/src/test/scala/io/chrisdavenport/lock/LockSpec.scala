package io.chrisdavenport.lock

import munit.CatsEffectSuite
import cats.effect._
import cats.data.Kleisli
import scala.concurrent.duration._

class LockSpec extends CatsEffectSuite {

  test("Lock Should Allow Re-entry") {
    for {
      lK <- Lock.reentrant[IO]
      l1 <- Unique[IO].unique.map(t => lK.mapK(Kleisli.applyK(t)))
      _ <- l1.lock >> l1.lock >> l1.lock
    } yield assert(true)
  }

  test("Should not allow others entry"){
    for {
      lK <- Lock.reentrant[IO]
      l1 <- Unique[IO].unique.map(t => lK.mapK(Kleisli.applyK(t)))
      l2 <- Unique[IO].unique.map(t => lK.mapK(Kleisli.applyK(t)))
      _ <- l1.lock
      out <- l2.tryLock
    } yield assertEquals(out, false, "Lock was acquired when it should not be")
  }

}
