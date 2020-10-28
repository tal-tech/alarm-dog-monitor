<?php

declare(strict_types=1);

namespace App\Common;

class Result
{
    const CODE_SUCCESS = 200;

    const CODE_ERROR = 500;

    const CODE_NOAUTH = 401;

    protected $resultArr = [
        'code' => -1,
        'msg' => '',
        'data' => '',
        'count' => 0,
    ];

    public function __construct()
    {
        $this->init(self::CODE_SUCCESS, 'success', []);
    }

    /**
     * @return $this
     */
    public function setCode(int $code)
    {
        $this->resultArr['code'] = $code;
        return $this;
    }

    public function getCode()
    {
        return $this->resultArr['code'];
    }

    /**
     * @return $this
     */
    public function setMsg(string $msg)
    {
        $this->resultArr['msg'] = $msg;
        return $this;
    }

    public function getMsg()
    {
        return $this->resultArr['msg'];
    }

    /**
     * @param $data
     * @return $this
     */
    public function setData($data)
    {
        $this->resultArr['data'] = $data;
        return $this;
    }

    public function getData()
    {
        return $this->resultArr['data'];
    }

    /**
     * @param $cnt
     * @return $this
     */
    public function setCnt($cnt)
    {
        $this->resultArr['count'] = $cnt;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getCnt()
    {
        return $this->resultArr['count'];
    }

    /**
     * @return $this
     */
    public function fillResult(\Exception $e)
    {
        $this->setCode($e->getCode());
        $this->setMsg($e->getMessage());
        return $this;
    }

    /**
     * @return array
     */
    public function toArray()
    {
        return $this->resultArr;
    }

    /**
     * return json.
     *
     * @param string $option
     * @return string
     */
    public function toJson($option = '')
    {
        return json_encode($this->resultArr, $option);
    }

    /**
     * @param array $data
     */
    private function init(int $code, string $msg = '', $data = [], int $cnt = 0)
    {
        $this->resultArr = [
            'code' => $code,
            'msg' => $msg,
            'data' => $data,
            'count' => $cnt,
        ];
    }
}
