<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

use App\Consumer\MonitorCallback\ConsumerCallback;

trait TraitCallback
{
    /**
     * @var ConsumerCallback
     */
    public $callback;

    /**
     * @return mixed
     */
    public function getCallback()
    {
        return $this->callback;
    }

    /**
     * @param mixed $callback
     */
    public function setCallback($callback): void
    {
        $this->callback = $callback;
    }
}
