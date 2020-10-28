<?php

declare(strict_types=1);

use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use Hyperf\Logger\LoggerFactory;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

return [
    'kafka_appid' => env('KAFKA_PRODUCT_APPID'),
    'kafka_appkey' => env('KAFKA_PRODUCT_APPKEY'),
    'alarm_api_uri' => env('ALARM_API_URI'),
    'gateway' => [
        'appid' => env('ALARM_GW_APPID'),
        'appkey' => env('ALARM_GW_APPKEY'),
    ],

    /*
     * GuzzleHttp配置
     */
    'guzzle' => [
        // guzzle原生配置选项
        'options' => [
            'connect_timeout' => 0,
            'timeout' => 0,
            // hyperf集成guzzle的swoole配置选项
            'swoole' => [
                'timeout' => 10,
                'socket_buffer_size' => 1024 * 1024 * 2,
            ],
        ],
        // guzzle中间件配置
        'middlewares' => [
            // // 失败重试中间件
            // 'retry' => function ($container = null) {
            //     return Middleware::retry(function ($retries, RequestInterface $request, ResponseInterface $response = null) {
            //         if (
            //             (! $response || $response->getStatusCode() >= 500) &&
            //             $retries < 1
            //         ) {
            //             return true;
            //         }
            //         return false;
            //     }, function () {
            //         return 10;
            //     });
            // },
            // 请求日志记录中间件
            'logger' => function ($container = null) {
                // $format中{response}调用$response->getBody()会导致没有结果输出
                $format = ">>>>>>>>\n{request}\n<<<<<<<<\n{res_headers}\n--------\n{error}";
                $formatter = new MessageFormatter($format);
                $logger = $container->get(LoggerFactory::class)->get('monitor', 'guzzle');

                return Middleware::log($logger, $formatter, 'debug');
            },
        ],
        // hyperf集成guzzle的连接池配置选项，非hyperf框架忽略
        'pool' => [
            'option' => [
                'max_connections' => 200,
            ],
        ],
    ],
];
