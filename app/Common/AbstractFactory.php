<?php

declare(strict_types=1);

namespace App\Common;

abstract class AbstractFactory
{
    abstract public static function getInstance($class);
}
