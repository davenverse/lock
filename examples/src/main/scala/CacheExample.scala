import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.syntax.all._
import cats.data._
import io.chrisdavenport.lock._

object CachedDataExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    CachedData.create[IO].flatMap(cd => 
      cd.processCachedData(i => IO(println(i)))
    )
    .as(ExitCode.Success)
  }

}

class CachedData[F[_]: Async](
  @volatile var cacheValid: Boolean,
  var data: Int,
  rwlK: ReadWriteLock[Kleisli[F, Unique.Token, *]]
){

  def processCachedData(use: Int => F[Unit]): F[Unit] = {
    Unique[F].unique.map(t => rwlK.mapK(Kleisli.applyK(t))).flatMap{rwl => 
      rwl.readLock.lock >> {
        if (!cacheValid){
          // Must release read lock before acquiring write lock
          rwl.readLock.unlock >>
          rwl.writeLock.permit.use(_ => 
            // Recheck state because another thread might have
            // acquired write lock and changed state before we did.
            Sync[F].delay{
              if (!cacheValid) {
                data = 10
                cacheValid = true
              } else ()
            } >> rwl.readLock.lock // Downgrade by acquiring read lock before releasing write lock
          ) // Unlock write, still hold read
        } else Applicative[F].unit
      } >> use(data).guarantee(rwl.readLock.unlock)
    
    }
  }
}

object CachedData {
  def create[F[_]: Async] = ReadWriteLock.reentrant[F].map(new CachedData[F](false, 1, _))
}


/*
 From ReentrantReadWriteLockExample
 class CachedData {
   Object data;
   volatile boolean cacheValid;
   final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

   void processCachedData() {
     rwl.readLock().lock();
     if (!cacheValid) {
        // Must release read lock before acquiring write lock
        rwl.readLock().unlock();
        rwl.writeLock().lock();
        try {
          // Recheck state because another thread might have
          // acquired write lock and changed state before we did.
          if (!cacheValid) {
            data = ...
            cacheValid = true;
          }
          // Downgrade by acquiring read lock before releasing write lock
          rwl.readLock().lock();
        } finally {
          rwl.writeLock().unlock(); // Unlock write, still hold read
        }
     }

     try {
       use(data);
     } finally {
       rwl.readLock().unlock();
     }
   }
 }
*/