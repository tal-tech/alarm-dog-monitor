<?php

declare(strict_types=1);

use App\Support\StdoutLogger;
use Hyperf\Contract\StdoutLoggerInterface;

return [
    StdoutLoggerInterface::class => StdoutLogger::class,
];
