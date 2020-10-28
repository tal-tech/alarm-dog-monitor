<?php

declare(strict_types=1);
require './vendor/autoload.php';
date_default_timezone_set('Asia/Shanghai');

try {
    $start = microtime(true);
    XesMq\Mq\Consumer::$logDir = '/tmp';
    $consumer = new XesMq\Mq\Consumer();
    $consumer->setProxy('127.0.0.1:8080');

    $consumer->setQueue('test_queue');
    $consumer->setGroup(json_encode([
        'vhost' => 'shovel',
        'username' => 'shovel',
        'password' => 'shovel',
    ]));
    $consumer->setReset('rabbitmq');

    var_dump($consumer->fetchRabbit());

    var_dump('耗时' . (microtime(true) - $start));
} catch (\Exception $e) {
    var_dump($e->getCode());
    var_dump($e->getMessage());
}
