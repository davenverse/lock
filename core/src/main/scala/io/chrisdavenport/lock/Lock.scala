package io.chrisdavenport.lock

import cats._
import cats.syntax.all._
import cats.data._
import cats.effect._
import cats.effect.syntax.all._
import cats.effect.std.Semaphore

trait Lock[F[_]]{ self => 
  def tryLock: F[Boolean]
  def lock: F[Unit]
  def unlock: F[Unit]
  def permit: Resource[F, Unit]
  def mapK[G[_]](fk: F ~> G)(implicit F: MonadCancel[F, _], G: MonadCancel[G, _]): Lock[G] = 
    new Lock[G]{
      def tryLock: G[Boolean] = fk(self.tryLock)

      def lock: G[Unit] = fk(self.lock)
      def unlock: G[Unit] = fk(self.unlock)
      def permit: Resource[G,Unit] = self.permit.mapK(fk)
    }
}

object Lock {
  def simple[F[_]: Concurrent]: F[Lock[F]] = 
    Semaphore[F](1).map{s => 
      new Lock[F]{
        def tryLock: F[Boolean] = s.tryAcquire
        def lock: F[Unit] = s.acquire
        def unlock: F[Unit] = s.release
        def permit = s.permit
      }
    }


  private case class Request[F[_], U](unique: U, gate: Deferred[F, Unit]) {
    def sameUnique(that: U)(implicit ev: Eq[U]) = that === unique
    def sameUnique(that: Request[F, U])(implicit ev: Eq[U]) = that.unique === unique
    def wait_ = gate.get
    def complete = gate.complete(())
  }
  private object Request {
    def create[F[_]: Concurrent, U](token: U): F[Request[F, U]] = 
      Deferred[F, Unit].map(Request(token, _))
  }
  

  import scala.collection.immutable.Queue
  private case class State[F[_], U](current: Option[Request[F, U]], waiting: Queue[Request[F, U]])

  private class KleisliReentrantLock[F[_]: Concurrent, U: Eq](ref: Ref[F, State[F, U]]) extends Lock[({ type M[A] = Kleisli[F, U, A]})#M]{

    def tryLock: Kleisli[F,U,Boolean] = Kleisli{(token: U) => 
      Request.create(token).flatMap{request =>
        ref.modify{
          case s@State(Some(main), waiting) => 
            if (request.sameUnique(main)) s -> Applicative[F].pure(true)
            else State(Some(main), waiting) -> Applicative[F].pure(false)
          case State(None, _) => (State(Some(request), Queue.empty), Applicative[F].pure(true))
        }.flatten
      }
    }.uncancelable

    def lock: Kleisli[F, U, Unit] = Kleisli{(token: U) => 
      Concurrent[F].uncancelable{ (poll: Poll[F]) => 
        Request.create(token).flatMap{request =>
          ref.modify{
            case s@State(Some(main), waiting) => 
              if (request.sameUnique(main)) s -> Applicative[F].unit
              else State(Some(main), waiting.enqueue(request)) -> 
                poll(request.gate.get).onCancel{
                  ref.update{
                    case s@State(Some(main), _) if request.sameUnique(main) => 
                      s
                    case State(s, wait2) =>
                      val wait = wait2.filterNot(request.sameUnique(_))
                      State(s, wait)
                  }
                }
            case State(None, _) => (State(Some(request), Queue.empty), Applicative[F].unit)
          }.flatten
        }
      }
    }

    def unlock: Kleisli[F, U, Unit] = Kleisli{(token: U) => 
        ref.modify{
          case State(Some(current), waiters) if current.sameUnique(token) => 
            waiters.dequeueOption match {
              case Some((head, tail)) => State(Some(head), tail) -> head.gate.complete(()).void
              case None => State(None, Queue.empty) -> Applicative[F].unit
            }
          case s@State(Some(holder), _) => 
            s -> new Exception(s"Not The Current Holder of this lock held by: ${holder.unique} you are $token").raiseError[F, Unit]
          case s@State(None, _) => s -> new Exception(s"Cannot Release a Lock you do not current hold").raiseError[F, Unit]
        }.flatten.uncancelable
    }

    def permit: Resource[({type M[A] = Kleisli[F, U, A]})#M, Unit] = 
      Resource.make(lock)(_ => unlock)
  }

  def reentrant[F[_]: Concurrent, U: Eq]: F[Lock[({ type M[A] = Kleisli[F, U, A]})#M]] = {
    Ref[F].of(State[F, U](None, Queue.empty)).map(new KleisliReentrantLock(_))
  }

  def reentrantUnique[F[_]: Concurrent]: F[Lock[({ type M[A] = Kleisli[F, Unique.Token, A]})#M]] =
    reentrant[F, Unique.Token]
    
  def reentrantBuildUnique[F[_]: Concurrent](lock: Lock[({ type M[A] = Kleisli[F, Unique.Token, A]})#M]): F[Lock[F]] = 
    Unique[F].unique.map(token => lock.mapK(Kleisli.applyK(token)))

  private def fromLocal[A](ioLocal: IOLocal[A]): ({type M[B] = Kleisli[IO, A, B]})#M ~> IO = new (({type M[B] = Kleisli[IO, A, B]})#M ~> IO){
    def apply[B](fa: Kleisli[IO,A,B]): IO[B] = ioLocal.get.flatMap(fa.run(_))
  }

  def ioLocal[U: Eq](ioLocal: IOLocal[U]): IO[Lock[IO]] = reentrant[IO, U].map(rwLockK => 
    rwLockK.mapK(fromLocal(ioLocal))
  )

}

