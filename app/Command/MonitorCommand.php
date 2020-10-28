<?php

declare(strict_types=1);

namespace App\Command;

use App\Consumer\MonitorConsumer;
use Hyperf\Command\Annotation\Command;
use Hyperf\Command\Command as HyperfCommand;
use Psr\Container\ContainerInterface;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Runtime;

/**
 * @Command
 */
class MonitorCommand extends HyperfCommand
{
    /**
     * @var ContainerInterface
     */
    protected $container;

    /**
     * Execution in a coroutine environment.
     *
     * @var bool
     */
    protected $coroutine = false;

    /**
     * @var MonitorConsumer
     */
    private $consumer;

    /**
     * MonitorCommand constructor.
     */
    public function __construct(
        ContainerInterface $container,
        MonitorConsumer $consumer
    ) {
        $this->container = $container;
        $this->consumer = $consumer;

        parent::__construct('alarm:monitor');
    }

    public function configure()
    {
        parent::configure();
    }

    /**
     * handle.
     */
    public function handle()
    {
        $process = new Process(function (Process $process) {
            Runtime::enableCoroutine(true, SWOOLE_HOOK_ALL | SWOOLE_HOOK_CURL);
            $this->consumer->process = $process;
            $this->consumer->start();
        }, false, 0, true);
        $process->start();

        $status = Process::wait(true);
        echo "Recycled #{$status['pid']}, code={$status['code']}, signal={$status['signal']}" . PHP_EOL;
    }
}
