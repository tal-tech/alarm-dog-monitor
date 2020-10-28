<?php

declare(strict_types=1);

namespace App\Model;

trait TraitDataSource
{
    /**
     * @var queue type
     */
    public $queue_type;

    /**
     * @return array
     */
    public function getDataSource()
    {
        $dataSource = [];
        $dataSourceObj = $this->dataSource();
        $sourceObj = $dataSourceObj->first();
        if (! empty($sourceObj)) {
            $dataSource = $sourceObj->toArray();
        }

        return $dataSource;
    }
}
