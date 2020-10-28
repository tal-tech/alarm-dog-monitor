<?php

declare(strict_types=1);

return [
    'handler' => [
        'http' => [
            App\Exception\Handler\AppExceptionHandler::class,
        ],
    ],
];
