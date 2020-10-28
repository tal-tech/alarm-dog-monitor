<?php

declare(strict_types=1);
require './vendor/autoload.php';
date_default_timezone_set('Asia/Shanghai');
try {
    $start = microtime(true);

    $producer = new XesMq\Mq\Producer();
    $producer->setAppid('111111');
    $producer->setAppkey('xxxxxxxxxxxxxxx');
    $producer->setQueue('test_delay');

    $producer->setProxy('127.0.0.1:8080');
    $producer->setTimeout(3);

    $extra = [];
    $extra['mq_type'] = 'rabbitmq';
    $extra['vhost'] = 'shovel';
    $extra['username'] = 'shovel';
    $extra['password'] = 'shovel';

    $extra['exchanges'] = [];
    $exchange = [];
    $exchange['name'] = 'test_delay';
    $exchange['type'] = 'x-delayed-message'; // direct fanout x-delayed-message
    $exchange['durable'] = 'true';
    $extra['exchanges'][] = $exchange;

    $extra['queues'] = [];
    $queue = [];
    $queue['name'] = 'test_queue';
    $queue['durable'] = 'true';
    $extra['queues'][] = $queue;

    $extra['bindings'] = [];
    $binding = [];
    $binding['queue'] = 'test_queue';
    $binding['exchange'] = 'test_delay';
    $binding['routingkey'] = 'test_route';
    $extra['bindings'][] = $binding;

    // x-delay only for x-delayed-message
    $extra['x-delay'] = 5000;

    var_dump($producer->produceRabbit('hello rabbitmq', 'test_route', null, null, $extra));

    var_dump('耗时' . (microtime(true) - $start));
} catch (\Exception $e) {
    var_dump($e->getCode());
    var_dump($e->getMessage());
}
