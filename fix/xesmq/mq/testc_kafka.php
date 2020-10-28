<?php

declare(strict_types=1);
require './vendor/autoload.php';
date_default_timezone_set('Asia/Shanghai');

try {
    $start = microtime(true);
    XesMq\Mq\Consumer::$logDir = '/tmp';
    $consumer = new XesMq\Mq\Consumer();
    $consumer->setProxy('127.0.0.1:8080');

    $consumer->setQueue('mq-pressure-test');
    $consumer->setGroup('test');
    $consumer->setReset('earliest');
    $consumer->setCommitTimeout(10);
    $consumer->setMaxConsumeTimes(2);
    //$consumer->setCommitMode(XesMq\Mq\Consumer::COMMIT_MODE_OFFSET);

    $callback = function ($data) use ($consumer) {
        var_dump(date('Y-m-d H:i:s') . ' 消费到消息,条数' . count($data));

        $offsets = [];
        foreach ($data as $item) {
            var_dump($item['queue'] . ':' . $item['props']['partition'] . ':' . $item['props']['offset']);
            var_dump('此消息是第' . $item['retry'] . '次重复消费');

            var_dump('时间戳类型:' . $item['props']['timestampType'] . ' , 时间戳:' . $item['props']['timestamp']);
            XesMq\Mq\Consumer::markSuccess($offsets, $item['queue'], $item['props']['partition'], $item['props']['offset']);
            //这样也可以
            //$consumer->markSuccess($offsets, $item['queue'], $item['props']['partition'], $item['props']['offset']);

            var_dump('==================');
        }
        //var_dump($offsets);
        return true;
        //return $offsets;
    };

    $consumer->setCallback($callback);

    //register signal to stop
    //pcntl_signal(SIGUSR2, function () use ($consumer) {
    //   $consumer->stopConsume();
    //});

    $consumer->consume();

    var_dump('耗时' . (microtime(true) - $start));
} catch (\Exception $e) {
    var_dump($e->getCode());
    var_dump($e->getMessage());
}
