/*
 * Copyright (c) 2026 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.concurrent;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SpscLinkedArrayQueue}.
 *
 * <p>The {@code concurrent*} tests document the SPSC contract violation behind
 * <a href="https://github.com/reactor/reactor-core/issues/2960">issue #2960</a>:
 * when two threads concurrently traverse a link boundary in
 * {@link SpscLinkedArrayQueue#poll()}, one thread's
 * {@code a.lazySet(m + 1, null)} (clears the link to the next array) wipes the
 * value the other thread just read into its local {@code b}, producing
 * {@code NullPointerException} at {@code o = b.get(offset)}.
 *
 * <p>While the queue is documented as single-producer / single-consumer, the
 * actual reactor operator chain {@code groupBy().flatMap(g -> g.publishOn(..))}
 * exposes the consumer side to multiple threads via {@code FluxPublishOn}'s
 * cleanup paths (drain loop's {@code checkTerminated} + {@code trySchedule}'s
 * cancelled branch), making this race observable in production.
 *
 * <p>These tests are expected to PASS while the bug is unfixed (the NPE is
 * captured) and to FAIL once {@code clear()}/{@code poll()} are guarded against
 * concurrent invocation, at which point they should be re-purposed as
 * regression tests verifying safety of the new contract.
 */
public class SpscLinkedArrayQueueTest {

	private static final int LINK_SIZE = 8;
	private static final int PREFILL_PAST_LINKS = 64;
	private static final int RACE_ITERATIONS = 2000;

	@Test
	@Tag("slow")
	void concurrentClearRacesAtLinkBoundary() {
		AtomicReference<Throwable> captured = runRace((q, race) -> race.run(q::clear, q::clear));

		assertNpeFromQueuePoll(captured.get(),
				"concurrent SpscLinkedArrayQueue.clear() — issue #2960 SPSC violation");
	}

	@Test
	@Tag("slow")
	void concurrentPollAndClearRacesAtLinkBoundary() {
		AtomicReference<Throwable> captured = runRace((q, race) -> race.run(q::clear, q::poll));

		assertNpeFromQueuePoll(captured.get(),
				"concurrent SpscLinkedArrayQueue.clear()/poll() — issue #2960 SPSC violation");
	}

	private static void assertNpeFromQueuePoll(Throwable captured, String description) {
		assertThat(captured)
				.as("expected NPE from %s", description)
				.isInstanceOf(NullPointerException.class);
		assertThat(captured.getStackTrace())
				.as("NPE must originate inside SpscLinkedArrayQueue.poll (link-boundary race)")
				.isNotEmpty();
		StackTraceElement top = captured.getStackTrace()[0];
		assertThat(top.getClassName()).isEqualTo(SpscLinkedArrayQueue.class.getName());
		assertThat(top.getMethodName()).isEqualTo("poll");
	}

	private static AtomicReference<Throwable> runRace(RaceScenario scenario) {
		AtomicReference<Throwable> captured = new AtomicReference<>();

		for (int i = 0; i < RACE_ITERATIONS && captured.get() == null; i++) {
			SpscLinkedArrayQueue<Integer> q = new SpscLinkedArrayQueue<>(LINK_SIZE);
			for (int v = 0; v < PREFILL_PAST_LINKS; v++) {
				q.offer(v);
			}

			scenario.execute(q, (a, b) -> RaceTestUtils.race(safe(a, captured), safe(b, captured)));
		}

		return captured;
	}

	private static Runnable safe(Runnable r, AtomicReference<Throwable> sink) {
		return () -> {
			try {
				r.run();
			}
			catch (Throwable t) {
				sink.compareAndSet(null, t);
			}
		};
	}

	@FunctionalInterface
	private interface RaceScenario {
		void execute(SpscLinkedArrayQueue<Integer> queue, RaceRunner race);
	}

	@FunctionalInterface
	private interface RaceRunner {
		void run(Runnable a, Runnable b);
	}
}
