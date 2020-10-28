<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

class BDataSource
{
    /**
     * @var
     */
    protected $running = true;

    /**
     * @param $time
     * @return bool
     */
    public function toSleep($time)
    {
        while ($time) {
            if ($this->running) {
                \Swoole\Coroutine::sleep(1);
                $time--;
            } else {
                return false;
            }
        }

        return true;
    }
}
