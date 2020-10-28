<?php

declare(strict_types=1);

namespace App\Model;

use Hyperf\DbConnection\Model\Model;

/**
 * @property int $id 自增ID
 * @property int $task_id 关联告警任务ID
 * @property string $name 监控任务名称
 * @property string $remark 备注
 * @property string $token 后面开放接口鉴权用
 * @property int $datasource_id 数据源ID
 * @property int $agg_cycle 聚合周期，单位秒，可枚举
 * @property string $config 监控配置
 * @property string $alarm_condition 告警条件
 * @property int $status 监控任务状态：1-启动；0-停止
 * @property int $created_by 创建人ID
 * @property \Carbon\Carbon $created_at 创建时间
 * @property \Carbon\Carbon $updated_at 更新时间
 * @property \App\Model\Datasource $dataSource
 */
class MonitorUniversal extends Model
{
    use TraitDataSource;

    /**
     * The table associated with the model.
     *
     * @var string
     */
    protected $table = 'monitor_universal';

    /**
     * The attributes that are mass assignable.
     *
     * @var array
     */
    protected $fillable = ['id', 'task_id', 'name', 'remark', 'token', 'datasource_id', 'agg_cycle', 'config', 'alarm_condition', 'status', 'created_by', 'created_at', 'updated_at'];

    /**
     * The attributes that should be cast to native types.
     *
     * @var array
     */
    protected $casts = ['id' => 'integer', 'task_id' => 'integer', 'datasource_id' => 'integer', 'agg_cycle' => 'integer', 'status' => 'integer', 'created_by' => 'integer', 'created_at' => 'datetime', 'updated_at' => 'datetime'];

    /**
     * 运行中的topic数量.
     *
     * @return \Hyperf\Database\Model\Relations\HasOne
     */
    public function dataSource()
    {
        return $this->hasOne(Datasource::class, 'id', 'datasource_id');
    }
}
