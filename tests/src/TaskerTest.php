<?php

  use PHPUnit\Framework\TestCase;

  final class TaskerTest extends TestCase {

    public function setup() {
      \Redis::clean('*');
    }

    public function testNonSslConnectionWithRedis() {
      new \Tasker();
    }

    // public function testSslConnectionWithRedis() {
    //   new \Tasker([
    //     'scheme' => 'tls'
    //   ]);
    // }

    public function testAdd() {
      $t = \NormalTask::perform_async([
        'arg' => rand(0, 1000)
      ]);

      $this->assertEquals(
        substr(\Redis::getKeys()[0], 0, -5),
        substr($t->key(), 0, -5)
      );
    }

    public function testTaskKey() {
      $t = \NormalTask::perform_async([
        'arg' => rand(0, 1000)
      ]);

      $this->assertEquals(
        'php-tasker:NormalTask',
        substr($t->key(), 0, 21)
      );
    }

    public function testRemove() {
      $t = \NormalTask::perform_async([
        'arg' => rand(0, 1000)
      ]);

      // $this->assertNotEmpty(\Redis::getKeys());

      // $t->remove($t);
      // $this->assertEmpty(\Redis::getKeys());
    }

    public function testRun() {
      \NormalTask::perform_async([
        'arg' => rand(0, 1000)
      ]);

      $t = new \Tasker();
      $t->stopGracefully();
      $t->run();
    }

    public function stopGracefully() {
      $t = new \Tasker();
      $this->assertFalse($t->shouldStop());
      $t->stopGracefully();
      $this->assertTrue($t->shouldStop());
    }
  }