<?php

declare(strict_types=1);

namespace App\Model;

use Hyperf\DbConnection\Model\Model;

class AlarmTaskModel extends Model
{
    const STOPED = 0;

    const RUNNING = 1;

    const PAUSE = 2;

    /**
     * 收敛指标（收敛方式）：1-条件收敛；2-智能收敛；3-全量收敛.
     * @var array
     */
    protected $compressMethod = [
        1 => '条件收敛',
        2 => '智能收敛',
        3 => '全量收敛',
    ];

    /**
     * The table associated with the model.
     *
     * @var string
     */
    protected $table = 'alarm_task';

    /**
     * The attributes that are mass assignable.
     *
     * @var array
     */
    protected $fillable = [
        'id',
        'name',
        'token',
        'department_id',
        'flag_save_db',
        'enable_workflow',
        'enable_filter',
        'enable_compress',
        'enable_upgrade',
        'enable_recovery',
        'status',
    ];
}
