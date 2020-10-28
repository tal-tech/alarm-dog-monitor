<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

use App\Common\DetectsLostConnections;
use App\Consumer\MonitorCallback\MysqlMonitorCallback;
use App\Consumer\MonitorConsumer;
use Hyperf\Database\Connectors\ConnectionFactory;
use Hyperf\Database\MySqlConnection;
use Hyperf\Database\Query\Builder;
use Hyperf\DbConnection\Db;
use Hyperf\Utils\Coroutine;

class MysqlDataSource extends BDataSource implements IDataSource
{
    use TraitMonitorInfo;
    use TraitConstruct;
    use TraitCallback;
    use TraitTriggerCycle;
    use DetectsLostConnections;

    /**
     * @var
     */
    private $mysqlClient;

    /**
     * @var array
     */
    private $config;

    /**
     * @var MySqlConnection
     */
    private $connection;

    /**
     * @return mixed
     */
    public function start()
    {
        try {
            startRun:
            if ($this->running) {
                $monitorTaskInfo = $this->getMonitorInfo();
                $taskModel = $monitorTaskInfo->getModel();
                $datasource = $monitorTaskInfo->getDataSource();
                $aggCycle = $taskModel->agg_cycle;
                $calcTime = $this->calcMinuteTriggerCycle();
                $start = $calcTime['start'];
                $end = $calcTime['end'];
                $toSleep = $calcTime['tosleep'];
                $timestampUnit = $datasource['timestamp_unit'];
                $timestampField = $datasource['timestamp_field'];
                if ($toSleep) {
                    $this->stdOutLogger->info('【monitor`】' . $monitorTaskInfo->getTaskFlag() . ' sleep:' . $toSleep);
                    if (! $this->toSleep($toSleep)) {
                        goto startRun;
                    }
                }
                switch ($timestampUnit) {
                    case 2:
                        $start = $start . '000';
                        $end = $end . '000';
                        break;
                    case 3:
                        $start = $start . '000000';
                        $end = $end . '000000';
                        break;
                    case 4:
                        $start = date('Y-m-d H:i:s', $start);
                        $end = date('Y-m-d H:i:s', $end);
                        break;
                }

                $query = $this->connection->table($this->config['table']);
                $query->where(function (Builder $query) use ($timestampField, $start, $end) {
                    $query->where($timestampField, '>=', $start);
                    $query->where($timestampField, '<', $end);
                });

                $filterConfig = [];
                $filterConfig = empty($taskModel->config) ? [] : json_decode($taskModel->config, true);
                if (isset($filterConfig['filter']['conditions'])) {
                    $filterConfig = $filterConfig['filter']['conditions'];
                }
                $alarmCondition = empty($taskModel->alarm_condition) ? [] : json_decode($taskModel->alarm_condition, true);
                if (isset($alarmCondition['conditions'])) {
                    $alarmCondition = $alarmCondition['conditions'];
                }
                $selectStr = '';
                foreach ($alarmCondition as $key => $arr) {
                    foreach ($arr['rule'] as $ruleKey => $oneRule) {
                        switch ($oneRule['agg_method']) {
                            case 'avg':
//                                $str = "avg(" . $oneRule['field'] . ") as " . $oneRule['field'] . "avg";
                                $str = 'avg(' . $oneRule['field'] . ') as ' . $oneRule['field'];
                                break;
                            case 'max':
//                                $str = "max(" . $oneRule['field'] . ") as " . $oneRule['field'] . "max";
                                $str = 'max(' . $oneRule['field'] . ') as ' . $oneRule['field'];
                                break;
                            case 'min':
//                                $str = "min(" . $oneRule['field'] . ") as " . $oneRule['field'] . "min";
                                $str = 'min(' . $oneRule['field'] . ') as ' . $oneRule['field'];
                                break;
                            case 'sum':
//                                $str = "sum(" . $oneRule['field'] . ") as " . $oneRule['field'] . "sum";
                                $str = 'sum(' . $oneRule['field'] . ') as ' . $oneRule['field'];
                                break;
                            case 'count':
//                                $str = "count(" . $oneRule['field'] . ") as " . $oneRule['field'] . "count";
                                $str = 'count(' . $oneRule['field'] . ') as ' . $oneRule['field'];
                                break;
                        }
                        $selectStr .= $str . ',';
                    }
                }
                $selectStr = substr($selectStr, 0, strlen($selectStr) - 1);
                $query->where(function (Builder $query) use ($filterConfig) {
                    foreach ($filterConfig as $key => $arr) {
                        $rules = $arr['rule'];
                        $query->orWhere(function (Builder $query) use ($rules) {
                            foreach ($rules as $key => $value) {
                                $this->filterConditon($query, $value);
                            }
                        });
                    }
                });
                $this->stdOutLogger->info('universal mysql data source getBindings: ' . json_encode($query->getBindings()));
                $this->stdOutLogger->info('universal mysql data source toSql: ' . $query->toSql());
                continueRun:
                try {
                    $resp = $query->selectRaw(Db::raw($selectStr))->get()->toArray();
                } catch (\Throwable $e) {
                    if ($this->causedByLostConnection($e)) {
                        $this->closeConnect();
                        $this->connect();
                        \Swoole\Coroutine::sleep(self::WAIT_BEFORE_RECONNECT);
                        goto continueRun;
                    }

                    throw $e;
                }
                $msgs = get_object_vars($resp[0]);
                $msgs = MonitorConsumer::filterData($monitorTaskInfo->getModel()->datasource_id, $msgs);
                $this->stdOutLogger->info('universal mysql data source query: ' . json_encode($msgs));
                $this->callback->handle($msgs);
                goto startRun;
            }
        } catch (\Throwable $e) {
            $msg = 'Coroutine ' . Coroutine::id() . ' mysql data source throw error ' . $e->getMessage() . '===' . $e->getTraceAsString();
            $this->dataSourceLogger->error($msg);
            $this->stdOutLogger->error($msg);
            \Swoole\Coroutine::sleep(self::WAIT_BEFORE_RECONNECT);
            $this->start();
        }
    }

    /**
     * @param $query Builder
     * @param $rules array
     * @return bool
     */
    public function filterConditon($query, $rules)
    {
        switch ($rules['operator']) {
            case 'eq':
                $query->where($rules['field'], $rules['threshold']);
                break;
            case 'gt':
                $query->where($rules['field'], '>', $rules['threshold']);
                break;
            case 'gte':
                $query->where($rules['field'], '>=', $rules['threshold']);
                break;
            case 'lt':
                $query->where($rules['field'], '<', $rules['threshold']);
                break;
            case 'lte':
                $query->where($rules['field'], '<=', $rules['threshold']);
                break;
            case 'neq':
                $query->where($rules['field'], '!=', $rules['threshold']);
                break;
            case 'in':
                $query->whereIn($rules['field'], $rules['threshold']);
                break;
            case 'not-in':
                $query->whereNotIn($rules['field'], $rules['threshold']);
                break;
        }
    }

    /**
     * @return mixed
     */
    public function stop()
    {
        try {
            $this->running = false;
        } catch (\Throwable $e) {
            $msg = 'kafka database stop exception ' . $e->getMessage() . ' at ' . date('Y-m-d H:i:s');
            $this->dataSourceLogger->error($msg);
            $this->stdOutLogger->error($msg);
        }
        return true;
    }

    /**
     * @return mixed
     */
    public function init()
    {
        /** @var MysqlMonitorCallback $callback */
        $callback = make(MysqlMonitorCallback::class);
        $rc = new \ReflectionClass(MysqlMonitorCallback::class);
        $rc->hasMethod('setLogger') && $callback->setLogger($this->dataSourceLogger);
        $rc->hasMethod('setMonitorInfo') && $callback->setMonitorInfo($this->getMonitorInfo())->init(); //模调相关配置
        $this->setCallback($callback);
        $monitorTaskInfo = $this->getMonitorInfo();
        $this->config = $monitorTaskInfo->getDataSource()['config'];
        if (! empty($this->config)) {
            $this->config = json_decode((string) $this->config, true);
        }
        $this->connect();
    }

    /**
     * 连接.
     */
    public function connect()
    {
        try {
            if (! is_null($this->connection)) {
                return;
            }
            $config = [
                'driver' => 'mysql',
                'host' => $this->config['host'],
                'port' => $this->config['port'],
                'prefix' => '',
                'database' => $this->config['database'],
                'username' => $this->config['username'] ?: config('databases.default.read.username'),
                'password' => $this->config['password'] ?: config('databases.default.read.password'),
                'charset' => 'utf8',
            ];
            $this->connection = $this->container->get(ConnectionFactory::class)->make($config);
        } catch (\Throwable $e) {
            $msg = 'mysql data source connect error exception ' . $e->getMessage() . ' at ' . date('Y-m-d H:i:s');
            $this->stdOutLogger->error($msg);
            $this->dataSourceLogger->error($msg);
            $this->connect();
            \Swoole\Coroutine::sleep(self::WAIT_BEFORE_RECONNECT);
        }
    }

    /**
     * close connect.
     */
    public function closeConnect()
    {
        unset($this->connection);
    }
}
