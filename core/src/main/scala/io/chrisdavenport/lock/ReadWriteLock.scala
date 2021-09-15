package io.chrisdavenport.lock

import cats._
import cats.syntax.all._
import cats.data._
import cats.effect._
import cats.effect.syntax.all._
import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration

trait ReadWriteLock[F[_]]{ self => 
  def readLock: Lock[F]
  def writeLock: Lock[F]
  def mapK[G[_]](fk: F ~> G)(implicit F: MonadCancel[F, _], G: MonadCancel[G, _]): ReadWriteLock[G] = 
    new ReadWriteLock[G] {
      def readLock: Lock[G] = self.readLock.mapK(fk)
      def writeLock: Lock[G] = self.writeLock.mapK(fk)
    }
}
object ReadWriteLock {
  private case class Request[F[_]](unique: Unique.Token, gate: Deferred[F, Unit]) {
    def sameUnique(that: Unique.Token) = that === unique
    def sameUnique(that: Request[F]) = that.unique === unique
    def wait_ = gate.get
    def complete = gate.complete(())
  }
  private object Request {
    def create[F[_]: Concurrent](token: Unique.Token): F[Request[F]] = 
      Deferred[F, Unit].map(Request(token, _))
  }


  private sealed trait Current[F[_]]
  private object Current {
    case class Reads[F[_]](running: Queue[Request[F]]) extends Current[F]
    case class Write[F[_]](running: Request[F]) extends Current[F]
  }
  private case class State[F[_]](
    current: Option[Current[F]],
    writeWaiting: Queue[Request[F]],
    readWaiting: Queue[Request[F]]
  )

  private class ReadWriteLockImpl[F[_]: Concurrent](ref: Ref[F, State[F]]) extends ReadWriteLock[({type M[A] = Kleisli[F, Unique.Token, A]})#M]{

    class ReadLock extends Lock[({type M[A] = Kleisli[F, Unique.Token, A]})#M]{

      def tryLock: Kleisli[F, Unique.Token, Boolean] = Kleisli{(token: Unique.Token) => 
        Request.create(token).flatMap{ req => 
          Concurrent[F].uncancelable{(poll: Poll[F]) => 
            ref.modify{
              case State(None, _, _) => 
                // No one with any locks
                State(Current.Reads(Queue(req)).some, Queue.empty, Queue.empty) -> Applicative[F].pure(true)
              case s@State(Some(Current.Reads(queue)), writeQueue, _) if writeQueue.isEmpty && queue.exists(req.sameUnique(_)) => 
                // Reentrant read
                s -> Applicative[F].pure(true)
              case s@State(Some(Current.Write(write)), writes, reads) if write.sameUnique(req) => 
                // On Downgrade Check if we've previously downgraded
                if (reads.exists(_.sameUnique(req))) s -> Applicative[F].pure(true)
                else {
                  // Downgrade Lock Will now appear in read and write locks
                  State(Current.Write(write).some, writes, reads.enqueue(write)) -> Applicative[F].pure(true)
                }

              case s@State(Some(Current.Reads(queue)), writeQueue, _) if writeQueue.isEmpty =>
                // Current Reading with no waiting writes, continue
                State(Current.Reads(queue.enqueue(req)).some, Queue.empty, Queue.empty) -> Applicative[F].pure(true)
              case s@State(Some(state), writeQueue, readQueue) => 
                s -> Applicative[F].pure(false)
            }.flatten
          }
        }
      }

      def lock: Kleisli[F, Unique.Token, Unit] = Kleisli{(token: Unique.Token) => 
        Request.create(token).flatMap{ req => 
          Concurrent[F].uncancelable{(poll: Poll[F]) => 
            ref.modify{
              case State(None, _, _) => 
                // No one with any locks
                State(Current.Reads(Queue(req)).some, Queue.empty, Queue.empty) -> Applicative[F].unit
              case s@State(Some(Current.Reads(queue)), writeQueue, _) if writeQueue.isEmpty && queue.exists(req.sameUnique(_)) => 
                // Reentrant read
                s -> Applicative[F].unit 
              case s@State(Some(Current.Write(write)), writes, reads) if write.sameUnique(req) => 
                // On Downgrade Check if we've previously downgraded
                if (reads.exists(_.sameUnique(req))) s -> Applicative[F].unit
                else {
                  // Downgrade Lock Will now appear in read and write locks
                  State(Current.Write(write).some, writes, reads.enqueue(write)) -> Applicative[F].unit
                }
                
              case s@State(Some(Current.Reads(queue)), writeQueue, _) if writeQueue.isEmpty =>
                // Current Reading with no waiting writes, continue
                State(Current.Reads(queue.enqueue(req)).some, Queue.empty, Queue.empty) -> Applicative[F].unit
              case s@State(Some(state), writeQueue, readQueue) => 
                State(state.some, writeQueue, readQueue.enqueue(req)) -> 
                  poll(req.wait_).onCancel{
                    ref.update{
                      case State(current, write, read) => 
                        val reads = read.filterNot(req.sameUnique)
                        State(current, write, reads)
                    }
                  }
            }.flatten
          }
        }
      }
      // Unlock out of Reads favors Writes
      def unlock: Kleisli[F, Unique.Token, Unit] = Kleisli{(token: Unique.Token) => 
        ref.modify{
          case State(Some(Current.Reads(queue)), writes, reads) => 
            val newCurrentRead = queue.filterNot(_.sameUnique(token))
            if (newCurrentRead.isEmpty){
              writes.dequeueOption match {
                case Some((head, tail)) =>  
                  State(Current.Write(head).some, tail, reads) -> head.complete.void
                case None => State(None, writes, reads) -> Applicative[F].unit
              }
            } else State(Current.Reads(newCurrentRead).some, writes, reads) -> Applicative[F].unit
          case s@State(Some(Current.Write(req)), _, _) => 
            s -> new Exception(s"Lock is held by write, cannot unlock a read").raiseError[F, Unit]
          case s@State(None, _,_) => 
            s -> new Exception(s"No Lock presently, cannot unlock when no lock is held").raiseError[F, Unit]
        }.flatten.uncancelable
      }
      def permit: Resource[({type M[A] = Kleisli[F, Unique.Token, A]})#M, Unit] = Resource.make(lock)(_ => unlock)
      
    }
    class WriteLock extends Lock[({type M[A] = Kleisli[F, Unique.Token, A]})#M]{

      def tryLock: Kleisli[F, Unique.Token, Boolean] = Kleisli{(token: Unique.Token) => 
        Request.create(token).flatMap{ req => 
            ref.modify{
              case State(None, _, _) => 
                // No one with any locks
                State(Current.Write(req).some, Queue.empty, Queue.empty) -> Applicative[F].pure(true)
              case s@State(Some(Current.Write(r)), _, _) if r.sameUnique(req) => 
                // Re-entrant write
                s -> Applicative[F].pure(true)
              case s@State(Some(other), writes, reads) => 
                s -> Applicative[F].pure(false)
            }.flatten
            .uncancelable
        }
      }
      def lock: Kleisli[F, Unique.Token, Unit] = Kleisli{(token: Unique.Token) => 
        Request.create(token).flatMap{ req => 
          Concurrent[F].uncancelable{(poll: Poll[F]) => 
            ref.modify{
              case State(None, _, _) => 
                // No one with any locks
                State(Current.Write(req).some, Queue.empty, Queue.empty) -> Applicative[F].unit
              case s@State(Some(Current.Write(r)), _, _) if r.sameUnique(req) => 
                // Re-entrant write
                s -> Applicative[F].unit
              case s@State(Some(other), writes, reads) => 
                State(other.some, writes.enqueue(req), reads) -> 
                  poll(req.wait_)
                    .onCancel(
                      ref.update{
                        case State(current, writes, reads) => 
                          State(current, writes.filterNot(req.sameUnique), reads)
                      }
                    )
            }
          }
        }
      }
      // Favor Batch Reads on Write Unlocks
      def unlock: Kleisli[F, Unique.Token, Unit] = Kleisli{(token: Unique.Token) => 
        ref.modify{
          case State(Some(Current.Write(req)), writes, reads) if req.sameUnique(token) => 
            if (reads.nonEmpty){
              State(Current.Reads(reads).some, writes, Queue.empty) -> 
                reads.traverse(_.complete.void)
            } else writes.dequeueOption match {
              case Some((head, tail)) => 
                State(Current.Write(head).some, tail, Queue.empty) -> head.complete.void
              case None => 
                State(None, Queue.empty, Queue.empty) -> Applicative[F].unit
            }
          case s@State(None, _, _) => s -> new Exception(s"Cannot unlock write lock when no lock is held").raiseError[F, Unit]
          case s@State(Some(Current.Reads(_)), _, _) => s -> new Exception(s"Cannot Unlock Write Lock when Read holds lock").raiseError[F, Unit]
          case s@State(Some(Current.Write(_)), _, _) => s -> new Exception(s"Another Write Holds Lock presently, cannot unlock").raiseError[F, Unit]
        }
      }
      def permit: Resource[({type M[A] = Kleisli[F, Unique.Token, A]})#M, Unit] = Resource.make(lock)(_ =>  unlock)

    }
    val readLock = new ReadLock
    val writeLock = new WriteLock

  }

  def reentrant[F[_]: Concurrent]: F[ReadWriteLock[({type M[A] = Kleisli[F, Unique.Token, A]})#M]] = 
    Concurrent[F].ref(State[F](None, Queue.empty, Queue.empty)).map(
      new ReadWriteLockImpl(_)
    )

  private def fromLocal[A](ioLocal: IOLocal[A]): ({type M[B] = Kleisli[IO, A, B]})#M ~> IO = new (({type M[B] = Kleisli[IO, A, B]})#M ~> IO){
    def apply[B](fa: Kleisli[IO,A,B]): IO[B] = ioLocal.get.flatMap(fa.run(_))
  }
  
  def ioLocal(ioLocal: IOLocal[Unique.Token]): IO[ReadWriteLock[IO]] = reentrant[IO].map(rwLockK => 
    rwLockK.mapK(fromLocal(ioLocal))
  )

}