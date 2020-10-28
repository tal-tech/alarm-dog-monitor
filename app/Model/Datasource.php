<?php

declare(strict_types=1);

namespace App\Model;

use Hyperf\DbConnection\Model\Model;

class Datasource extends Model
{
    /**
     * 数据源类型.
     */
    const TYPE_ES = 1;

    const TYPE_MYSQL = 2;

    const TYPE_KAFKA = 3;

    const TYPE_WEBHOOK = 4;

    /**
     * 监控类型.
     */
    const MONITOR_UNIVERSAL = 'universal';

    const MONITOR_CYCLE_COMPARE = 'cycle_compare';

    const MONITOR_PROTOCOL_DETECT = 'protocol_detect';

    public static $types = [
        self::TYPE_ES => 'ElasticSearch',
        self::TYPE_MYSQL => 'MySQL',
        self::TYPE_KAFKA => 'Kafka',
        self::TYPE_WEBHOOK => 'Webhook',
    ];

    public static $monitors = [
        self::MONITOR_UNIVERSAL => '通用监控',
        self::MONITOR_CYCLE_COMPARE => '同比环比监控',
        self::MONITOR_PROTOCOL_DETECT => '心跳探活',
    ];

    /**
     * The table associated with the model.
     *
     * @var string
     */
    protected $table = 'monitor_datasource';

    /**
     * The attributes that are mass assignable.
     *
     * @var array
     */
    protected $fillable = [
        'id',
        'type',
        'name',
        'remark',
        'config',
        'fields',
        'timestamp_field',
        'timestamp_unit',
        'created_by',
        'created_at',
        'updated_at',
    ];

    /**
     * The attributes that should be cast to native types.
     *
     * @var array
     */
    protected $casts = [];
}
