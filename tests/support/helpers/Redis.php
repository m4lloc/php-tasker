<?php

  class Redis {

    private static $c = false;

    public static function getInstance() {
      if(!self::$c) {
        self::$c = new \Predis\Client();
      }
      return self::$c;
    }

    public static function getKeys($pattern='*') {
      return self::getInstance()->keys($pattern);
    }

    public static function clean($pattern='*') {
      $keys = self::getInstance()->keys($pattern);
      foreach($keys as $k) {
        self::getInstance()->del($k);
      }
    }
  }