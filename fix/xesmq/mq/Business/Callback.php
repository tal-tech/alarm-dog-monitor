<?php

declare(strict_types=1);

namespace XesMq\Mq\Business;

interface Callback
{
    public function handle($msg);
}
