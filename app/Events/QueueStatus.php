<?php

declare(strict_types=1);

namespace App\Events;

use App\Consumer\MonitorConsumer;

class QueueStatus
{
    public $consumerProcess;

    public function __construct(MonitorConsumer $consumerProcess)
    {
        $this->consumerProcess = $consumerProcess;
    }
}
