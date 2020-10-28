<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

interface IDataSource
{
    const WAIT_BEFORE_RECONNECT = 1;

    /**
     * @return mixed
     */
    public function start();

    /**
     * @return mixed
     */
    public function stop();

    /**
     * @return mixed
     */
    public function init();
}
