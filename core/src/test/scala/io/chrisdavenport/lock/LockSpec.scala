package io.chrisdavenport.lock

import munit.CatsEffectSuite
import cats.effect._
import cats.data.Kleisli
import cats.syntax.all._
import scala.concurrent.duration._

class LockSpec extends CatsEffectSuite {

  test("Lock Should Allow Re-entry") {
    for {
      lK <- Lock.reentrant[IO]
      l1 <- Lock.rentrantUnique(lK)
      _ <- l1.lock >> l1.lock >> l1.lock
    } yield assert(true)
  }

  test("Lock Should not allow others entry"){
    for {
      lK <- Lock.reentrant[IO]
      l1 <- Lock.rentrantUnique(lK)
      l2 <- Lock.rentrantUnique(lK)
      _ <- l1.lock
      out <- l2.tryLock
    } yield assertEquals(out, false, "Lock was acquired when it should not be")
  }

  test("Lock Should allow others access after unlock"){
    for {
      lK <- Lock.reentrant[IO]
      l1 <- Lock.rentrantUnique(lK)
      l2 <- Lock.rentrantUnique(lK)
      _ <- l1.lock
      out1 <- l2.tryLock
      _ <- l1.unlock
      out2 <- l2.tryLock
    } yield assertEquals((out1, out2), (false, true))
  }

  test("Lock Should unlock waiters on unlock"){
    for {
      lK <- Lock.reentrant[IO]
      l1 <- Lock.rentrantUnique(lK)
      l2 <- Lock.rentrantUnique(lK)
      _ <- l1.lock
      out <- (l2.lock, Temporal[IO].sleep(1.second) >> l1.unlock).parTupled
    } yield assertEquals(true, true, "Good if we don't lock above")
  }

}
