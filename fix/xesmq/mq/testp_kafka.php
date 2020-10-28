<?php

declare(strict_types=1);
require './vendor/autoload.php';
date_default_timezone_set('Asia/Shanghai');
try {
    $start = microtime(true);

    $producer = new XesMq\Mq\Producer();
    $producer->setAppid('11111');
    $producer->setAppkey('xxxxxxxxxxxxxx');
    $producer->setQueue('mq-pressure-test');
    $producer->setProxy('127.0.0.1:8080');
    $producer->setTimeout(3);

    for ($i = 0; $i < 10000; $i++) {
        $timestr = date('Y-m-d H:i:s');
        var_dump($timestr);
        //$producer->produce($timestr, null, null, null, null);
        var_dump($producer->produce($timestr, null, null, null, []));
        //$producer->batchAdd($timestr, null, null, null, ['x-delay' => '1m']);
    }
    //var_dump($producer->batchProduce());
    //$producer->batchClear();

    var_dump('耗时' . (microtime(true) - $start));
} catch (\Exception $e) {
    var_dump($e->getCode());
    var_dump($e->getMessage());
}
