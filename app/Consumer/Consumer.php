<?php

declare(strict_types=1);

namespace App\Consumer;

use Hyperf\Contract\StdoutLoggerInterface;
use Psr\Log\LoggerInterface;

abstract class Consumer
{
    /**
     * @var StdoutLoggerInterface
     */
    protected $stdOutLogger;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * RabbitMqConsumer constructor.
     */
    public function __construct()
    {
    }

    /**
     * start.
     */
    abstract public function start();
}
