<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

use App\Consumer\MonitorCallback\EsMonitorCallback;
use App\Consumer\MonitorConsumer;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Hyperf\Database\Query\Builder;
use Hyperf\Elasticsearch\ClientBuilderFactory;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\Coroutine;

class EsDataSource extends BDataSource implements IDataSource
{
    use TraitMonitorInfo;
    use TraitConstruct;
    use TraitCallback;
    use TraitTriggerCycle;

    /**
     * @var array
     */
    private $config;

    /**
     * @var
     */
    private $connection;

    /**
     * @var Client
     */
    private $esClient;

    /**
     * @return mixed
     */
    public function start()
    {
        startRun:
        try {
            if ($this->running) {
                $monitorTaskInfo = $this->getMonitorInfo();
                $calcTime = $this->calcMinuteTriggerCycle();
                $start = $calcTime['start'];
                $end = $calcTime['end'];
                $toSleep = $calcTime['tosleep'];
                $taskModel = $monitorTaskInfo->getModel();
                $datasource = $monitorTaskInfo->getDataSource();
                $timestampField = $datasource['timestamp_field'];
                $timestampUnit = $datasource['timestamp_unit'];

                $filterConfig = [];
                $filterConfig = empty($taskModel->config) ? [] : json_decode($taskModel->config, true);
                if (isset($filterConfig['filter']['conditions'])) {
                    $filterConfig = $filterConfig['filter']['conditions'];
                }
                $alarmCondition = empty($taskModel->alarm_condition) ? [] : json_decode($taskModel->alarm_condition, true);
                if (isset($alarmCondition['conditions'])) {
                    $alarmCondition = $alarmCondition['conditions'];
                }
                if ($toSleep) {
                    $this->stdOutLogger->info('【monitor`】' . $monitorTaskInfo->getTaskFlag() . ' sleep:' . $toSleep);
                    if (! $this->toSleep($toSleep)) {
                        goto startRun;
                    }
                }
                $params = [];
                $params['index'] = $this->renderIndex();
//                $params['type'] = 'test';
                $params['_source']['excludes'] = [];
                $dataFormat = '';
                switch ($timestampUnit) {
                    case 1:
//                        $dataFormat = 'epoch_second';
                        $dataFormat = '';
                        break;
                    case 2:
                    case 3:
                        $start = $start . '000';
                        $end = $end . '000';
//                      $dataFormat = 'epoch_millis';
                        $dataFormat = '';
                        break;
                    case 4:
                        $start = $start . '000';
                        $end = $end . '000';
                        $dataFormat = 'epoch_millis';
                        break;
                }
                $params['body']['query']['bool']['must'][0]['range'][$timestampField] = [
                    'lt' => $end,
                    'gte' => $start,
                ];

                if ($dataFormat) {
                    $params['body']['query']['bool']['must'][0]['range'][$timestampField]['format'] = $dataFormat;
//                    $params['body']['query']['bool']['must'][0]['range'][$timestampField]['time_zone'] = '+8:00';
                }

                $params['body']['query']['bool']['must'][1]['bool']['minimum_should_match'] = 1;
                foreach ($filterConfig as $key => $arr) {
                    $rules = $arr['rule'];
                    $bool = $this->filterConditon($rules);
                    $params['body']['query']['bool']['must'][1]['bool']['should'][]['bool'] = $bool;
                }
                $alarmConditionArr = [];
                foreach ($alarmCondition as $condition) {
                    foreach ($condition['rule'] as $conditionRule) {
                        $params['body']['aggs'][$conditionRule['field'] . '_stats']['stats']['field'] = $conditionRule['field'];
                        $alarmConditionArr[$conditionRule['field']][] = $conditionRule['agg_method'];
                    }
                }

                $params['body']['from'] = 0;
                //size 0 聚合仍然生效，只是不返回数据
                $params['body']['size'] = 0;
                $this->stdOutLogger->info('es query: ' . json_encode($params));
                $ret = $this->esClient->search($params);
                $msgs = [];
                $aggregations = empty($ret['aggregations']) ? [] : $ret['aggregations'];
                $this->stdOutLogger->info('aggs result' . json_encode($aggregations));
                if (! empty($aggregations)) {
                    foreach ($alarmConditionArr as $aggField => $aggArr) {
                        foreach ($aggArr as $index => $aggValue) {
                            //@todo 支持同一个字段支持多种聚合方式
                            //$msgs[$aggField . $aggValue] = isset($aggregations[$aggField.'_stats'][$aggValue]) ? round($aggregations[$aggField.'_stats'][$aggValue]) : 0;
                            //@todo 支持同一个字段支持一种聚合方式
                            $msgs[$aggField] = isset($aggregations[$aggField . '_stats'][$aggValue]) ? round($aggregations[$aggField . '_stats'][$aggValue]) : 0;
                        }
                    }
                    $msgs = MonitorConsumer::filterData($taskModel->datasource_id, $msgs);
                    $this->stdOutLogger->info('aggs handler after' . json_encode($msgs));
                    $this->callback->handle($msgs);
                }
                goto startRun;
            }
        } catch (\Throwable $e) {
            $msg = 'Coroutine ' . Coroutine::id() . ' Es data source throw error ' . $e->getMessage() . '===' . $e->getTraceAsString();
            $this->dataSourceLogger->error($msg);
            $this->stdOutLogger->error($msg);
            \Swoole\Coroutine::sleep(self::WAIT_BEFORE_RECONNECT);
            goto startRun;
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
     * @param $query Builder
     * @param $rules array
     * @return array
     */
    public function filterConditon($rules)
    {
        $bool = [];
        foreach ($rules as $rule) {
            switch ($rule['operator']) {
                case 'eq':
                    $bool['must'][]['match_phrase'][$rule['field']] = $rule['threshold'];
                    break;
                case 'neq':
                    $bool['must_not'][]['match_phrase'][$rule['field']] = $rule['threshold'];
                    break;
                case 'gt':
                    $bool['must'][]['range'][$rule['field']]['gt'] = $rule['threshold'];
                    break;
                case 'gte':
                    $bool['must'][]['range'][$rule['field']]['gte'] = $rule['threshold'];
                    break;
                case 'lt':
                    $bool['must'][]['range'][$rule['field']]['lt'] = $rule['threshold'];
                    break;
                case 'lte':
                    $bool['must'][]['range'][$rule['field']]['lte'] = $rule['threshold'];
                    break;
                case 'in':
                    if (is_array($rule['threshold'])) {
                        $should = [];
                        foreach ($rule['threshold'] as $oneValue) {
                            $should[]['match_phrase'][$rule['field']] = $oneValue;
                        }
                        $bool['must'][]['bool'] = [
                            'minimum_should_match' => 1,
                            'should' => $should,
                        ];
                    }
                    break;
                case 'not-in':
                    if (is_array($rule['threshold'])) {
                        $should = [];
                        foreach ($rule['threshold'] as $oneValue) {
                            $should[]['match_phrase'][$rule['field']] = $oneValue;
                        }
                        $bool['must_not'][]['bool'] = [
                            'minimum_should_match' => 1,
                            'should' => $should,
                        ];
                    }
                    break;
            }
        }
        return $bool;
    }

    /**
     * @return mixed
     */
    public function init()
    {
        /** @var EsMonitorCallback $callback */
        $callback = make(EsMonitorCallback::class);
        $rc = new \ReflectionClass(EsMonitorCallback::class);
        $rc->hasMethod('setLogger') && $callback->setLogger($this->dataSourceLogger);
        $rc->hasMethod('setMonitorInfo') && $callback->setMonitorInfo($this->getMonitorInfo())->init(); //模调相关配置
        $this->setCallback($callback);
        $monitorTaskInfo = $this->getMonitorInfo();
        $this->config = $monitorTaskInfo->getDataSource()['config'];
        if (! empty($this->config)) {
            $this->config = json_decode((string) $this->config, true);
        } else {
            $this->stdOutLogger->error('es data source is empty!');
            $this->dataSourceLogger->error('es data source is empty!');
        }
        $this->connect();
        $this->renderIndex();
    }

    /**
     * 连接.
     */
    public function connect()
    {
        if (! is_null($this->esClient)) {
            return;
        }

        $builder = $this->container->get(ClientBuilderFactory::class)->create();
        $hosts = array_map(function ($node) {
            return sprintf('http://%s:%s', $node['host'], $node['port']);
        }, $this->config['nodes']);

        $this->esClient = $this->createClient($builder, $hosts);
    }

    /**
     * Create Client.
     *
     * @return Client
     */
    protected function createClient(ClientBuilder $builder, array $hosts)
    {
        $clientBuilder = $builder->setHosts($hosts);
        if (config('datasource.es.enable_log')) {
            $clientBuilder->setLogger($this->container->get(LoggerFactory::class)->get('elasticsearch'));
        }
        return $clientBuilder->build();
    }

    /**
     * 渲染索引.
     *
     * @return string
     */
    protected function renderIndex()
    {
        $dates = explode('-', date('Y-m-d-H-i-s'));
        $vars = [
            'yyyy' => $dates[0],
            'MM' => $dates[1],
            'dd' => $dates[2],
            'HH' => $dates[3],
            'mm' => $dates[4],
            'ss' => $dates[5],
        ];

        $replaceSearch = [];
        $replaceVars = [];
        foreach ($this->config['index_vars'] as $varName) {
            $replaceSearch[] = '{' . $varName . '}';
            $replaceVars[] = $vars[$varName];
        }

        return str_replace($replaceSearch, $replaceVars, $this->config['index']);
    }
}
