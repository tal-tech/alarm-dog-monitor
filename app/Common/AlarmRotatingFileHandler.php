<?php

declare(strict_types=1);

namespace App\Common;

use Monolog\Handler\RotatingFileHandler;

class AlarmRotatingFileHandler extends RotatingFileHandler
{
    protected $alarmHandler;

    public function __construct($filename, $maxFiles, $level, $bubble, $filePermission, $useLocking)
    {
        parent::__construct($filename, $maxFiles, $level, $bubble, $filePermission, $useLocking);
    }

    protected function write(array $record)
    {
        parent::write($record);
    }
}
