package io.chrisdavenport.lock

import munit.CatsEffectSuite
import cats.effect._
import cats.data.Kleisli
import cats.syntax.all._
import scala.concurrent.duration._

class ReadWriteLockSpec extends CatsEffectSuite {

  test("ReadWriteLock Write Should Allow WriteLock Re-entry") {
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.writeLock.lock >> l1.writeLock.lock >> l1.writeLock.lock
    } yield assert(true)
  }

  test("ReadWriteLock Write  Should not allow others write entry"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      l2 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.writeLock.lock
      out <- l2.writeLock.tryLock
    } yield assertEquals(out, false, "ReadWriteLock was acquired when it should not be")
  }

  test("ReadWriteLock Write Should allow others writers access after unlock"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      l2 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.writeLock.lock
      out1 <- l2.writeLock.tryLock
      _ <- l1.writeLock.unlock
      out2 <- l2.writeLock.tryLock
    } yield assertEquals((out1, out2), (false, true))
  }

  test("ReadWriteLock Write Should unlock write waiters on unlock"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      l2 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.writeLock.lock
      out <- (l2.writeLock.lock, Temporal[IO].sleep(1.second) >> l1.writeLock.unlock).parTupled
    } yield assertEquals(true, true, "Good if we don't lock above")
  }

  test("ReadWriteLock Write should unlock waiting reads"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      l2 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.writeLock.lock
      out <- (l2.readLock.lock, Temporal[IO].sleep(1.second) >> l1.writeLock.unlock).parTupled
    } yield assertEquals(true, true, "Good if we don't lock above")
  }

  test("ReadWriteLock Write Downgrade lock is still held"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      l2 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.writeLock.lock
      out1 <- l2.writeLock.tryLock
      _ <- l1.readLock.lock 
      _ <- l1.writeLock.unlock
      out2 <- l2.writeLock.tryLock
    } yield assertEquals((out1, out2), (false, false))
  }

  test("ReadWriteLock Write Downgrade lock is still held, but allows readers"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      l2 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.writeLock.lock
      out1 <- l2.readLock.tryLock
      _ <- l1.readLock.lock 
      _ <- l1.writeLock.unlock
      out2 <- l2.readLock.tryLock
    } yield assertEquals((out1, out2), (false, true))
  }

  test("ReadWriteLock Read Lock allows additional readers"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      l2 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.readLock.lock
      out <- l2.readLock.tryLock
    } yield assertEquals(out, true)
  }

  test("ReadWriteLock Read Lock is reentrant"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.readLock.lock
      out <- l1.readLock.tryLock
    } yield assertEquals(out, true)
  }

  test("ReadWriteLock ReadLock will wake write wait"){
    for {
      lK <- ReadWriteLock.reentrant[IO]
      l1 <- ReadWriteLock.rentrantUnique(lK)
      l2 <- ReadWriteLock.rentrantUnique(lK)
      _ <- l1.readLock.lock
      _ <- (l2.writeLock.lock, Temporal[IO].sleep(100.millis) >> l1.readLock.unlock).parTupled
    } yield assertEquals(true, true, "If we get here we're not block")
  }

}