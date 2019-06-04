package mongo

import (
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/obase/conf"
	"time"
)

const CKEY = "mongo"

// 对接conf.yml, 读取原redis相关配置
func init() {
	conf.Init()
	configs, ok := conf.GetSlice(CKEY)
	if !ok || len(configs) == 0 {
		return
	}

	for _, config := range configs {
		if key, ok := conf.ElemString(config, "key"); ok {
			address, ok := conf.ElemStringSlice(config, "address")
			database, ok := conf.ElemString(config, "database")
			username, ok := conf.ElemString(config, "username")
			password, ok := conf.ElemString(config, "password")
			source, ok := conf.ElemString(config, "source")
			safe, ok := conf.ElemMap(config, "safe")
			mode, ok := conf.Elem(config, "mode")

			keepalive, ok := conf.ElemDuration(config, "keepalive")
			if !ok {
				keepalive = time.Minute
			}
			connectTimeout, ok := conf.ElemDuration(config, "connectTimeout")
			if !ok {
				connectTimeout = 30 * time.Second
			}
			readTimeout, ok := conf.ElemDuration(config, "readTimeout")
			if !ok {
				readTimeout = 30 * time.Second
			}
			writeTimeout, ok := conf.ElemDuration(config, "writeTimeout")
			if !ok {
				writeTimeout = 30 * time.Second
			}
			minPoolSize, ok := conf.ElemInt(config, "minPoolSize")
			maxPoolSize, ok := conf.ElemInt(config, "maxPoolSize")
			if !ok {
				maxPoolSize = 16
			}
			maxPoolWaitTimeMS, ok := conf.ElemInt(config, "maxPoolWaitTimeMS")
			maxPoolIdleTimeMS, ok := conf.ElemInt(config, "maxPoolIdleTimeMS")

			defalt, ok := conf.ElemBool(config, "default")

			option := &Option{
				Address:           address,
				Database:          database,
				Username:          username,
				Password:          password,
				Source:            source,
				Safe:              getSafe(safe),
				Mode:              getMode(mode),
				ConnectTimeout:    connectTimeout,
				Keepalive:         keepalive,
				WriteTimeout:      writeTimeout,
				ReadTimeout:       readTimeout,
				MinPoolSize:       minPoolSize,
				MaxPoolSize:       maxPoolSize,
				MaxPoolWaitTimeMS: maxPoolWaitTimeMS,
				MaxPoolIdleTimeMS: maxPoolIdleTimeMS,
			}

			if err := Init(key, option, defalt); err != nil {
				panic(err)
			}
		}
	}
}

func getMode(val interface{}) mgo.Mode {
	switch val := val.(type) {
	case nil:
		return mgo.Eventual //默认返回最终一致
	case string:
		return GetMode(val)
	case int:
		return mgo.Mode(val)
	case uint:
		return mgo.Mode(val)
	default:
		return GetMode(fmt.Sprintf("%v", val))
	}
}

func getSafe(val map[string]interface{}) *mgo.Safe {
	safe := &mgo.Safe{
		WMode: Safe_majority,
	}
	for k, v := range val {
		switch k {
		case "W", "w":
			safe.W = conf.ToInt(v)
		case "WMode", "wmode":
			safe.WMode = conf.ToString(v)
		case "RMode", "rmode":
			safe.RMode = conf.ToString(v)
		case "WTimeout", "wtimeout":
			safe.WTimeout = conf.ToInt(v)
		case "FSync", "fsync":
			safe.FSync = conf.ToBool(v)
		case "J", "j":
			safe.J = conf.ToBool(v)
		}
	}
	return safe
}
