import cats.syntax.all._
import cats.effect._
import cats.data._
import io.chrisdavenport.lock._

object ReadWriteLockExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    ReadWriteLock.reentrant[IO].flatMap(rwlockK => 
      (Unique[IO].unique, Unique[IO].unique).tupled.flatMap{ case (u1, u2) => 
        val rwLock1 = rwlockK.mapK(Kleisli.applyK(u1))
        val rwLock2 = rwlockK.mapK(Kleisli.applyK(u2))
        rwLock1.writeLock.lock >>
        rwLock1.readLock.lock >> IO(println("Made it")) >>
        rwLock1.writeLock.unlock >>
        rwLock1.readLock.unlock
      }
    ).as(ExitCode.Success)
  }

}