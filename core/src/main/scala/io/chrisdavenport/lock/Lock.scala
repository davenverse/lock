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
  

  import scala.collection.immutable.Queue
  private case class State[F[_]](current: Option[Request[F]], waiting: Queue[Request[F]])

  private class KleisliReentrantLock[F[_]: Concurrent](ref: Ref[F, State[F]]) extends Lock[({ type M[A] = Kleisli[F, Unique.Token, A]})#M]{

    def tryLock: Kleisli[F,Unique.Token,Boolean] = Kleisli{(token: Unique.Token) => 
      Request.create(token).flatMap{request =>
        ref.modify{
          case s@State(Some(main), waiting) => 
            if (request.sameUnique(main)) s -> Applicative[F].pure(true)
            else State(Some(main), waiting) -> Applicative[F].pure(false)
          case State(None, _) => (State(Some(request), Queue.empty), Applicative[F].pure(true))
        }.flatten
      }
    }.uncancelable

    def lock: Kleisli[F, Unique.Token, Unit] = Kleisli{(token: Unique.Token) => 
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

    def unlock: Kleisli[F, Unique.Token, Unit] = Kleisli{(token: Unique.Token) => 
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

    def permit: Resource[({type M[A] = Kleisli[F, Unique.Token, A]})#M, Unit] = 
      Resource.make(lock)(_ => unlock)
  }

  def reentrant[F[_]: Concurrent]: F[Lock[({ type M[A] = Kleisli[F, Unique.Token, A]})#M]] = {
    Ref[F].of(State[F](None, Queue.empty)).map(new KleisliReentrantLock(_))
  }

  def rentrantUnique[F[_]: Concurrent](lock: Lock[({ type M[A] = Kleisli[F, Unique.Token, A]})#M]): F[Lock[F]] = 
    Unique[F].unique.map(token => lock.mapK(Kleisli.applyK(token)))

  private def fromLocal[A](ioLocal: IOLocal[A]): ({type M[B] = Kleisli[IO, A, B]})#M ~> IO = new (({type M[B] = Kleisli[IO, A, B]})#M ~> IO){
    def apply[B](fa: Kleisli[IO,A,B]): IO[B] = ioLocal.get.flatMap(fa.run(_))
  }

  def ioLocal(ioLocal: IOLocal[Unique.Token]): IO[Lock[IO]] = reentrant[IO].map(rwLockK => 
    rwLockK.mapK(fromLocal(ioLocal))
  )

}

