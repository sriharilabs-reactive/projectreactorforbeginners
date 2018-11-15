package be.reactiveprogramming.projectreactorforbeginners;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;
import reactor.test.StepVerifier;
import reactor.test.StepVerifier.Step;
import reactor.test.scheduler.VirtualTimeScheduler;

public class Exercises {

  /**
   * TODO 00 READ: Welcome to Project Reactor for beginners. We'll be doing a number exercises to get acquainted with this Reactive Streams library. The exercises are laid out in the form of tests
   * to be able to run them easily. These tests will try to verify your results as well as possible through the StepVerifier, but often you'll also have to check manually if they worked as inspected.
   *
   * Of course you can work in groups, this can certainly lead to interesting debate throughout the exercises.
   */

  /**
   * TODO 01 READ: The Flux is one of the most basic Publishers in Project Reactor, but one of the most versatile at the same time. It's a part of a pipeline where values will stream through in a
   * Reactive way. It will emit elements until it ends with an error or completes. Operations can be added to them which will "extend" the pipeline, and enables for more endpoints to be added. Most of the
   * time the first part of the pipeline will be on the asynchronous boundaries of the application, where values can be supplied to it in a non-blocking manner through Reactive drivers interfacing
   * with external systems.
   *
   * For simplicity's sake however, these exercises will be created with some basic values that will be provided in the Flux itself. Most of the exercises will have a StepVerifier at the end, which will
   * verify the results of your created Reactive pipelines. In other exercises, a description will tell you what you should expect to see at the end of the exercise. These exercises require you to use
   * a subscribe method at the end of your Reactive pipeline to activate it. This will work in a non-blocking wau, however, so you'll be required to use the sleep() method underneath this text to make
   * sure the thread doesn't stop by itself.
   */

  public void sleep(int seconds) {
    try {
      new CountDownLatch(1).await(seconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }
  }

  /**
   * TODO 02 Create a basic Flux that will emit the values 1, 2, 3, and 5, instead of just the value 1.
   */
  @Test
  public void someStarterValues() {
    Flux<Integer> startFlux = Flux.just(1, 2, 3, 5);

    StepVerifier.create(startFlux).expectNext(1, 2, 3, 5).verifyComplete();
  }

  /**
   * TODO 03 Create a basic Flux that will emit no values at all, without using "Flux.just()"
   */
  @Test
  public void noValuesAtAll() {
    Flux<Integer> startFlux = Flux.empty();

    StepVerifier.create(startFlux).expectComplete();
  }

  /**
   * TODO 04 Create a basic Flux that will emit the values of 1 to 1'000'000
   */
  @Test
  public void aMillionEvents() {
    Flux<Integer> startFlux = Flux.range(1, 1000000);

    StepVerifier.create(startFlux).expectNext(1, 2, 3, 4, 5).expectNextCount(999994).expectNext(1000000).verifyComplete();
  }

  /**
   * TODO 05 Reactive Streams need to be subscribed to before anything happens, before that the pipelines are just built in a declarative way. Create a Flux that will emit 1 to 10 and subscribe yourself
   * to it, using a lambda to print them out to the command line.
   *
   * TIP: Remember to use the sleep method to make the test Thread wait
   */
  @Test
  public void firstSubscription() {
    Flux<Integer> startFlux = Flux.range(1, 10);

    startFlux.subscribe(System.out::println);

    sleep(1);
  }

  /**
   * TODO 06 Not all Fluxes are supplied from an upstream Publisher, Fluxes can generate their own data as well. Create and subscribe to a Flux that will emit an increasing number every 500ms.
   */
  @Test
  public void everySecond() {
    Flux<Long> startFlux = Flux.interval(Duration.ofMillis(500));

    startFlux.subscribe(System.out::println);

    sleep(5);
  }

  /**
   * TODO 07 As you may have noticed in previous exercises, the StepVerifier offered by the reactor-test library makes testing a Reactive Stream far easier. Let's do the previous test again using
   * its VirtualTimeScheduler.
   */
  @Test
  public void notTooFast() {
    /*
     The virtualTimeScheduler will work together with the StepVerifier's withVirtualTime method. It enables us to pretend that time is moving forward in the application
     much faster than it actually is. This will make a test that would otherwise take 10 seconds only take a number of milliseconds.
    */

    VirtualTimeScheduler.getOrSet();

    Flux<Long> slowedTimeFlux = Flux.interval(Duration.ofSeconds(1));

    Step<Long> verifier = StepVerifier.withVirtualTime(() -> slowedTimeFlux.take(10)); //take(10) will only request the following 10 elements from the Flux

    for (long i = 0; i < 10; i++) {
      verifier = verifier.thenAwait(Duration.ofSeconds(1)).expectNext(i);
    }

    verifier.verifyComplete();
  }

  /**
   * TODO 08 READ Basic operators: We'll start looking at some basic operators that you can apply on Fluxes to change or filter out values. These are added onto the Reactive Pipelines through
   * a builder pattern, straight onto the Fluxes. These will be applied to the different data values emitted by the Flux, extending the Reactive Pipeline.
   */

  /**
   * TODO 09 We'll start with the concept of mappers. A mapper can be used to convert an input value of a Flux to an output value, similar to the map method of Java Streams
   * Use the Map function on a Flux to get the values of 2, 4, 6, 8 all the way to 1000000
   */
  @Test
  public void basicMapper() {
    Flux<Integer> startFlux =
        Flux.range(1, 500000)
            .map(i -> i * 2);

    StepVerifier.create(startFlux).expectNext(2, 4, 6, 8, 10).expectNextCount(499994).expectNext(1000000).verifyComplete();
  }

  /**
   * TODO 10 Next to mappers we can apply filters. These filters will either pass on the value emitted by the Flux or stop it, so it won't be emitted further downstream. Use a filter to make the
   * following test pass.
   */
  @Test
  public void basicFilter() {
    Flux<Integer> startFlux =
        Flux.range(1, 1000000)
            .filter(i -> i % 2 == 0 && i % 3 == 0);

    StepVerifier.create(startFlux).expectNext(6, 12, 18).expectNextCount(166662).expectNext(999996).verifyComplete();
  }

  /**
   * TODO 11 Try without changing the original Flux.range(1, 10) to make the following test pass (generating the uneven numbers between 1 and 20)
   */
  @Test
  public void chaining() {
    Flux<Integer> startFlux =
        Flux.range(1, 10)
            .map(i ->  i * 2)
            .filter(i -> i % 2 == 0)
            .map(i -> i - 1);

    StepVerifier.create(startFlux).expectNext(1, 3, 5, 7, 9).expectNextCount(4).expectNext(19).verifyComplete();
  }

  /**
   * TODO 12 Read: The Mono is very similar to a Flux, except instead of 0, 1, or many results, it will either return O, or 1 result before completing or throwing an error.
   */

  /**
   * TODO 13 This should be pretty simple by now. Instead of a Flux, let's create a very simple Mono.
   */
  @Test
  public void somethingSimple() {
    Mono<String> letterMono = Mono.just("a");

    StepVerifier.create(letterMono).expectNext("a").verifyComplete();
  }

  /**
   * TODO 14 Mono's are supplied by functionalities in Project Reactor as well. Verify the truth about the letter A in the letters Flux, using the Flux API
   */
  @Test
  public void fluxMonoResults() {
    Flux<String> letters = Flux.just("A", "B", "C");

    Mono<Boolean> anyLetterIsA = letters.any(a -> a.equals("A"));
    Mono<Boolean> allLettersAreA = letters.all(a -> a.equals("A"));

    StepVerifier.create(anyLetterIsA).expectNext(true).verifyComplete();
    StepVerifier.create(allLettersAreA).expectNext(false).verifyComplete();
  }

  /**
   * TODO 15 Both Fluxes and Monos can signal an error. Try to create a Mono that will make the following test pass.
   */
  @Test
  public void errors() {
    Mono<Object> errorMono = Mono.error(new IllegalArgumentException());

    StepVerifier.create(errorMono).verifyError(IllegalArgumentException.class);
  }

  /**
   * TODO 15 Now let's do the same, but only after getting good results first. No need to change the original Flux.just() either. Keep in mind that any normal exception being thrown will be passed
   * as an error, so let's try to trigger one.
   */
  @Test
  public void errorAfterSomeGoodResults() {
    Flux<Integer> errorAfterAWhileFlux = Flux.just(20, 25, 50, 100, 0)
        .map(i -> 100 / i);

    StepVerifier.create(errorAfterAWhileFlux).expectNext(5, 4, 2, 1).verifyError(ArithmeticException.class);
  }

  /**
   * TODO 16 For the following exercises on combining Monos, it's recommended to first read the following article:
   * https://www.reactiveprogramming.be/project-reactor-combining-monos/
   */

  /**
   * TODO 17 Try to combine a million Monos that all return a number into a Flux that will consume them sequentially
   */
  @Test
  public void aMillionMonos() {
    List<Mono<Integer>> monosList = new ArrayList<>();

    for(int i = 0; i < 1000000; i++) {
      monosList.add(Mono.just(5));
    }

    Flux<Integer> resultFlux = Flux.concat(monosList);

    StepVerifier.create(resultFlux).expectNextCount(1000000).verifyComplete();
  }

  /**
   * TODO 18 Create a Mono letterMono and a Mono numberMono that will combine to the letterNumberMono
   */
  @Test
  public void letterNumberMono() {
    Mono<String> letterMono = Mono.just("A");
    Mono<Integer> numberMono = Mono.just(1);

    Mono<String> letterNumberMono = Mono.zip(letterMono, numberMono, (l, n) -> l + n);

    StepVerifier.create(letterNumberMono).expectNext("A1").verifyComplete();
  }

  /**
   * TODO 19 For the following exercises on combining Fluxes, it's recommended to first read the following article:
   * https://www.reactiveprogramming.be/project-reactor-combining-fluxes/
   */

  /**
   * TODO 20 Combine a Flux of letters and a Flux of numbers into a lettersAndNumbers Flux
   */
  @Test
  public void combiningFluxElements() {
    Flux<String> letters = Flux.just("a", "b", "c", "d", "e");
    Flux<Integer> numbers = Flux.range(1, 5);

    Flux<String> lettersAndNumbers = Flux.zip(letters, numbers).map(t -> t.getT1() + t.getT2());

    StepVerifier.create(lettersAndNumbers).expectNext("a1", "b2", "c3", "d4", "e5").verifyComplete();
  }

  /**
   * TODO 21 Concatenate a Flux of letters and a Flux of numbers into a concatenatedLettersAndNumbers Flux
   */
  @Test
  public void concatenatingFluxElements() {
    Flux<String> letters = Flux.just("a", "b", "c", "d", "e");
    Flux<Integer> numbers = Flux.range(1, 5);

    Flux<String> lettersAndNumbers = Flux.concat(letters, numbers.map(i -> "" + i));

    StepVerifier.create(lettersAndNumbers).expectNext("a", "b", "c", "d", "e", "1", "2", "3", "4", "5").verifyComplete();
  }

  /**
   * TODO 22 Through combining with another Flux, make sure the startFlux only Streams with one letter every second
   */
  @Test
  public void slowLetters() {
    Flux<String> letters = Flux.just("a", "b", "c", "d", "e");
    Flux<Long> timeFlux = Flux.interval(Duration.ofSeconds(1));

    Flux<String> slowedLetters = Flux.zip(letters, timeFlux, (a, b) -> a);

    StepVerifier.create(slowedLetters).expectNext("a").thenAwait(Duration.ofSeconds(1)).expectNext("b").expectNextCount(2).expectNext("e").verifyComplete();
  }

  /**
   * TODO 23 For the following exercises on sharing Fluxes, it's recommended to first read the following article:
   * https://www.reactiveprogramming.be/project-reactor-flux-sharing/
   */

  /**
   * TODO 24 Use Flux sharing to split the lettersToShare flux up into two Fluxes that will be mapped and then concatenated again in the combinedStreams
   */
  @Test
  public void sharingTheInput() {
    Flux<String> lettersToShare = Flux.just("a", "b", "c", "d", "e").share();

    Flux<String> upperCaseStream = Flux.from(lettersToShare).map(String::toUpperCase);
    Flux<String> numberAddedStream = Flux.from(lettersToShare).zipWith(Flux.range(1, 5), (l, n) -> l + n);

    Flux<String> combinedStreams = Flux.concat(upperCaseStream, numberAddedStream);

    StepVerifier.create(combinedStreams).expectNext("A", "B", "C", "D", "E", "a1", "b2", "c3", "d4", "e5").verifyComplete();
  }

  /**
   * TODO 25 For the following exercises on using the TopicProcessor Fluxes, it's recommended to first read the following article:
   * https://www.reactiveprogramming.be/project-reactor-topicprocessor/
   */

  /**
   * TODO 26 Let's try to do the previous exercise again, but this time using a TopicProcessor instead of sharing the Flux.
   */
  @Test
  public void sharingTheInputThroughTopic() {
    Flux<String> lettersToShare = Flux.just("a", "b", "c", "d", "e");

    TopicProcessor<String> topicProcessor = TopicProcessor.create();
    lettersToShare.subscribe(topicProcessor);

    Flux<String> upperCaseStream = Flux.from(topicProcessor).map(String::toUpperCase);
    Flux<String> numberAddedStream = Flux.from(topicProcessor).zipWith(Flux.range(1, 5), (l, n) -> l + n);

    Flux<String> combinedStreams = Flux.concat(upperCaseStream, numberAddedStream);

    StepVerifier.create(combinedStreams).expectNext("A", "B", "C", "D", "E", "a1", "b2", "c3", "d4", "e5").verifyComplete();
  }

  /**
   * TODO 27 A little more challenging this time, perhaps. We'll compare regular Flux sharing with TopicProcessor when used at scale. For both, create a Reactive Stream from the lettersToShare
   * Flux that gets split up to a thousand new Fluxes. Do this first with regular Flux sharing, then with a TopicProcessor. Compare the results in speed. One will be faster than the other because
   * it will only run on the Threads provided by Project Reactor. The other one will have a Thread for each Subscriber. This should also give you an indication how "expensive" Thread switching is
   * when compared to just a couple of Threads that process events like in Project Reactor's event loop.
   */
  @Test
  public void comparingTheTwo() {
    Flux<Long> lettersToShare = Flux.interval(Duration.ofSeconds(1)).take(10).share();

    for(int i = 0; i < 1000; i++) {
      final int fluxNr = i;
      Flux.from(lettersToShare).map(n -> "Flux " + fluxNr + " " + n).subscribe(System.out::println);
    }

    sleep(10);

    Flux<Long> moreLettersToShare = Flux.interval(Duration.ofSeconds(1)).take(10);

    TopicProcessor<Long> topicProcessor = TopicProcessor.create();
    moreLettersToShare.subscribe(topicProcessor);

    for(int i = 0; i < 1000; i++) {
      final int fluxNr = i;
      Flux.from(topicProcessor).map(n -> "Flux " + fluxNr + " " + n).subscribe(System.out::println);
    }

    sleep(10);
  }

  /**
   * TODO 28 For the following exercises on caching Fluxes, it's recommended to first read the following article:
   * https://www.reactiveprogramming.be/project-reactor-flux-caching/
   */

  /**
   * TODO 29 As a final exercise, think up a nice usecase where the Flux cache() method can be applied, and give it a tryout in the following method sandbox :-)
   */
  @Test
  public void cacheSandbox() {
    Flux<Long> startFlux = Flux.interval(Duration.ofSeconds(1)).share().cache();

    Flux firstFlux = Flux.from(startFlux);
    firstFlux.subscribe(out -> System.out.println("firstFlux value: " + out));
    sleep(5);

    Flux secondFlux = Flux.from(startFlux);
    secondFlux.subscribe(out -> System.out.println("secondFlux value: " + out));
    sleep(5);
  }

  /**
   * TODO 30 READ: Congratulations, you're done with these basic exercises on Project Reactor. You've learned what Fluxes and Monos are, how to split and combine Reactive Streams, and map and filter
   * the elements that run through them.
   *
   * You can start setting your next steps in Reactive Programming now. There are many technologies available that integrate with Project Reactor, like Spring Webflux, MongoDB, RabbitMQ, Kafka,
   * and many others that you should definitely take a look at. Try to think up an interesting project where the concepts of Reactive Streams have a big advantage. Projects where high throughput
   * data streaming, like (nearly) real time applications, and distributed applications that have to be able scale out massively are very good candidates for this.
   */

}
