package kvmap

import (
    "encoding/json"
    "errors"
)

type KVmap struct {
    m map[string]string
}

func NewKVmap() *KVmap {
    kvmap := new(KVmap)
    kvmap.m = map[string]string{}
    return kvmap
}

func (kvmap *KVmap) Insert(key string, value string) bool {
    _, exist := kvmap.m[key]
    if exist {
        return false
    }
    kvmap.m[key] = value
    return true
}

func (kvmap *KVmap) Delete(key string) (bool, string) {
    original_value, exist := kvmap.m[key]
    if !exist {
        return false, ""
    }
    delete(kvmap.m, key)
    return true, original_value
}

func (kvmap *KVmap) Get(key string) (bool, string) {
    original_value, exist := kvmap.m[key]
    if !exist {
        return false, ""
    }
    return true, original_value
}

func (kvmap *KVmap) Update(key string, value string) bool {
    _, exist := kvmap.m[key]
    if !exist {
        return false
    }
    kvmap.m[key] = value
    return true
}

func (kvmap *KVmap) CountKey() int {
    return len(kvmap.m)
}

func (kvmap *KVmap) ToString() string {
    cnt := 0
    size := len(kvmap.m)
    info := "["
    for key, value := range kvmap.m {
        info += "[\"" + key + "\",\"" + value + "\"]"
        cnt++
        if cnt < size {
            info += ","
        }
    }
    info += "]"
    return info
}

func (kvmap *KVmap) Serialize() []byte {
    return []byte(kvmap.ToString())
}

func (kvmap *KVmap) Unserialize(data []byte) error {
    kvmap.m = map[string]string{}

    var tmp [][]string
    err := json.Unmarshal(data, &tmp)
    if err != nil {
        return err
    }

    for i := 0; i < len(tmp); i++ {
        if (len(tmp[i]) != 2) {
            kvmap.m = map[string]string{}
            return errors.New("cannot unserialize data")
        }
        kvmap.m[tmp[i][0]] = tmp[i][1]
    }
    return nil
}
