import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.syntax.all._
import cats.data._
import java.util.TreeMap
import io.chrisdavenport.lock._

object RWDictionaryExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    RWDictionary.create[IO].flatMap(rw => 
      rw.allKeys.flatTap(l => IO(println(l))) >>
      rw.put("yellow", 3) >>
      rw.put("what", 4) >>
      rw.allKeys.flatTap(l => IO(println(l))) >>
      rw.get("yellow").flatTap(i => IO(println(i)))
    )
    .as(ExitCode.Success)
  }

}

class RWDictionary[F[_]: Async](
  val m: TreeMap[String, Int] = new TreeMap[String, Int],
  val rwlK: ReadWriteLock[Kleisli[F, Unique.Token, *]]
){
  import scala.collection.JavaConverters._
  def getLock: F[ReadWriteLock[F]] = Unique[F].unique.map(t => rwlK.mapK(Kleisli.applyK(t)))

  def get(key: String): F[Int] = getLock.flatMap(_.readLock.permit.use(_ =>
    Sync[F].delay(m.get(key))
  ))

  def allKeys: F[List[String]] = getLock.flatMap(_.readLock.permit.use(_ => 
    Sync[F].delay(m.keySet().asScala.toList)
  ))

  def put(key: String, data: Int): F[Unit] = getLock.flatMap(_.writeLock.permit.use(_ => 
    Sync[F].delay(m.put(key, data))
  ))

  def clear: F[Unit] = getLock.flatMap(_.writeLock.permit.use(_ => 
    Sync[F].delay(m.clear())
  ))

}

object RWDictionary {
  def create[F[_]: Async]: F[RWDictionary[F]] = ReadWriteLock.reentrant[F].map(k =>
    new RWDictionary(new TreeMap[String, Int], k)
  )
}

/*
ReentrantReadWriteLocks can be used to improve concurrency in some uses of some kinds of Collections. 
This is typically worthwhile only when the collections are expected to be large, 
accessed by more reader threads than writer threads, 
and entail operations with overhead that outweighs synchronization overhead.

For example, here is a class using a TreeMap that is expected to be large and concurrently accessed.

class RWDictionary {
    private final Map<String, Data> m = new TreeMap<String, Data>();
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    public Data get(String key) {
        r.lock();
        try { return m.get(key); }
        finally { r.unlock(); }
    }
    public String[] allKeys() {
        r.lock();
        try { return m.keySet().toArray(); }
        finally { r.unlock(); }
    }
    public Data put(String key, Data value) {
        w.lock();
        try { return m.put(key, value); }
        finally { w.unlock(); }
    }
    public void clear() {
        w.lock();
        try { m.clear(); }
        finally { w.unlock(); }
    }
 }
*/