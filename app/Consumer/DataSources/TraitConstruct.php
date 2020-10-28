<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Di\Container;
use Hyperf\Logger\LoggerFactory;

trait TraitConstruct
{
    /**
     * @var Container
     */
    public $container;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    public $dataSourceLogger;

    /**
     * @var StdoutLoggerInterface
     */
    public $stdOutLogger;

    /**
     * KafkaConsumerInstance constructor.
     */
    public function __construct(
        Container $container,
        LoggerFactory $loggerFactory,
        StdoutLoggerInterface $stdOutlogger
    ) {
        $this->container = $container;
        $this->dataSourceLogger = $loggerFactory->get('data_source', 'default');
        $this->stdOutLogger = $stdOutlogger;
    }
}
