/*
 * Copyright (C) 2011 the original author or authors. See the notice.md file distributed with this
 * work for additional information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.iq80.leveldb.util;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Stream;

import org.iq80.leveldb.AsynchronousIterator;

public final class CompletableFutures {
  private CompletableFutures() {}

  public static <T, U> CompletionStage<Stream<U>> flatMapIterator(
      final AsynchronousIterator<T> iter, final Function<T, U> mapper) {
    return iteratorStep(iter, new ConcurrentLinkedQueue<U>(), mapper);
  }

  @SuppressWarnings("unchecked")
  private static <T, U> CompletionStage<Stream<U>> iteratorStep(final AsynchronousIterator<T> iter,
      final Collection<U> c, final Function<T, U> mapper) {
    return iter.next().thenCompose(next -> {
      if (next.isPresent()) {
        c.add(mapper.apply(next.get()));
        return iteratorStep(iter, c, mapper);
      } else {
        return CompletableFuture.completedFuture(Stream.of((U[]) c.toArray()));
      }
    });
  }

  public static <T> CompletionStage<Stream<T>> allOf(final Stream<CompletionStage<T>> stages) {
    @SuppressWarnings("unchecked")
    final CompletableFuture<T>[] futures =
        (CompletableFuture<T>[]) stages.map(CompletionStage::toCompletableFuture).toArray();

    return CompletableFuture.allOf(futures)
        .thenApply(voided -> Stream.of(futures).map(CompletableFuture::join));
  }

  /**
   * Like {@link CompletionStage#thenCompose(Function)} except that the second stage is always
   * executed on the completion of the first stage (normal or exceptional). If the first stage
   * completes with an exception, the second stage will be run, but the returned stage will fail
   * upon the second's completion with the first stage's exception. If both the first and the second
   * stage complete with exceptions, the returned stage will fail with the first stage's exception
   * and the second stage's exception added as a suppressed exception
   * <p>
   * Think of this as an async "try/finally" where the first stage is within the "try" block and the
   * second stage is within the "finally"
   */
  public static <T, U> CompletionStage<U> composeUnconditionally(final CompletionStage<T> first,
      final Function<? super T, ? extends CompletionStage<U>> then) {
    final CompletableFuture<U> f = new CompletableFuture<U>();
    first.whenComplete((t, tException) -> {
      then.apply(t).whenComplete((u, uException) -> {
        if (tException != null) {
          if (uException != null) {
            tException.addSuppressed(uException);
          }
          f.completeExceptionally(tException);
        } else if (uException != null) {
          f.completeExceptionally(uException);
        } else {
          f.complete(u);
        }
      });
    });
    return f;
  }

  public static <T> CompletableFuture<T> exceptionalFuture(final Throwable ex) {
    final CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(ex);
    return f;
  }

}