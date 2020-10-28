<?php

declare(strict_types=1);
require './vendor/autoload.php';
date_default_timezone_set('Asia/Shanghai');
try {
    $start = microtime(true);

    $producer = new XesMq\Mq\Producer();
    $producer->setAppid('1111');
    $producer->setAppkey('xxxxxxxxxxxxx');
    //$producer->setQueue('mq-pressure-test');
    $producer->setProxy('127.0.0.1:8080');
    $producer->setTimeout(3);

    var_dump($producer->createTopic('create-topic-test', 3, 2, null));

    var_dump('耗时' . (microtime(true) - $start));
} catch (\Exception $e) {
    var_dump($e->getCode());
    var_dump($e->getMessage());
}
