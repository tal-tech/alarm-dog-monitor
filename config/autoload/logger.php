<?php

declare(strict_types=1);

return [
    'default' => [
        'handler' => [
            'class' => \App\Common\AlarmRotatingFileHandler::class,
            'constructor' => [
                'filename' => env('LOG_PATH', BASE_PATH . '/runtime/logs') . '/hyperf.log',
                'level' => Monolog\Logger::NOTICE,
                'maxFiles' => 5,
                'bubble' => true,
                'filePermission' => null,
                'useLocking' => false,
            ],
        ],
        'formatter' => [
            'class' => Monolog\Formatter\JsonFormatter::class,
            'constructor' => [
                'format' => null,
                'dateFormat' => null,
                'allowInlineLineBreaks' => true,
            ],
        ],
    ],
    'guzzle' => [
        'handler' => [
            'class' => \Monolog\Handler\RotatingFileHandler::class,
            'constructor' => [
                'filename' => env('LOG_PATH', BASE_PATH . '/runtime/logs') . '/guzzle.line',
                'level' => (int) env('LOGGER_GUZZLE_LOG_LEVEL', Monolog\Logger::DEBUG),
                'maxFiles' => 5,
                'bubble' => true,
                'filePermission' => null,
                'useLocking' => false,
            ],
        ],
        'formatter' => [
            'class' => Monolog\Formatter\LineFormatter::class,
            'constructor' => [
                'format' => null,
                'dateFormat' => null,
                'allowInlineLineBreaks' => true,
            ],
        ],
    ],
];
