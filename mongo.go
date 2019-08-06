package mongo

import (
	"errors"
	"github.com/globalsign/mgo"
	"strings"
	"time"
)

type Bulk interface {
	Insert(docs ...interface{})
	Upsert(pairs ...interface{})
	RemoveOne(selectors ...interface{})
	RemoveAll(selectors ...interface{})
	UpdateOne(pairs ...interface{})
	UpdateAll(pairs ...interface{})
}

type BulkFunc func(bk Bulk, args ...interface{})
type SessionFunc func(se *mgo.Session, args ...interface{}) (interface{}, error)
type CollectionFunc func(cl *mgo.Collection, args ...interface{}) (interface{}, error)

type Mongo interface {
	Count(c string) (n int, err error)
	Indexes(c string) (indexes []mgo.Index, err error)
	EnsureIndex(c string, index mgo.Index) error
	EnsureIndexKey(c string, key ...string) error
	DropIndex(c string, key ...string) error
	DropIndexName(c string, name string) error

	// For whole document
	FindOne(c string, ret interface{}, query interface{}) (bool, error)
	FindAll(c string, ret interface{}, query interface{}, sort ...string) error
	FindRange(c string, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error
	FindPage(c string, tot *uint32, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error
	FindDistinct(c string, ret interface{}, query interface{}, key string, sort ...string) error
	FindId(c string, ret interface{}, id interface{}) (bool, error)
	// Find And Select
	SelectOne(c string, ret interface{}, query interface{}, projection interface{}) (bool, error)
	SelectAll(c string, ret interface{}, query interface{}, projection interface{}, sort ...string) error
	SelectRange(c string, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error
	SelectPage(c string, tot *uint32, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error
	SelectDistinct(c string, ret interface{}, query interface{}, projection interface{}, key string, sort ...string) error
	SelectId(c string, ret interface{}, id interface{}, projection interface{}) (bool, error)
	// FindAndModify
	FindAndUpdate(c string, ret interface{}, query interface{}, update interface{}) (updated int, err error)              // return old doucument
	FindAndUpsert(c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error)   // return old doucument
	FindAndRemove(c string, ret interface{}, query interface{}) (removed int, err error)                                  // return old doucument
	FindAndUpdateRN(c string, ret interface{}, query interface{}, update interface{}) (updated int, err error)            // return new doucument
	FindAndUpsertRN(c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) // return new doucument

	Insert(c string, docs ...interface{}) error
	RemoveOne(c string, selector interface{}) (bool, error)
	RemoveAll(c string, selector interface{}) (removed int, err error)
	RemoveId(c string, id interface{}) (bool, error)
	UpdateOne(c string, selector interface{}, update interface{}) (bool, error)
	UpdateAll(c string, selector interface{}, update interface{}) (updated int, err error)
	UpdateId(c string, id interface{}, update interface{}) (bool, error)
	UpsertOne(c string, selector interface{}, update interface{}) (upsertedId interface{}, err error)
	UpsertId(c string, id interface{}, update interface{}) (upsertedId interface{}, err error)
	RunBulk(c string, f BulkFunc, args ...interface{}) (matched int, modified int, err error)
	RunCollection(c string, f CollectionFunc, args ...interface{}) (interface{}, error)

	DBCount(d string, c string) (n int, err error)
	DBIndexes(d string, c string) (indexes []mgo.Index, err error)
	DBEnsureIndex(d string, c string, index mgo.Index) error
	DBEnsureIndexKey(d string, c string, key ...string) error
	DBDropIndex(d string, c string, key ...string) error
	DBDropIndexName(d string, c string, name string) error

	// For whole document
	DBFindOne(d string, c string, ret interface{}, query interface{}) (bool, error)
	DBFindAll(d string, c string, ret interface{}, query interface{}, sort ...string) error
	DBFindRange(d string, c string, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error
	DBFindPage(d string, c string, tot *uint32, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error
	DBFindDistinct(d string, c string, ret interface{}, query interface{}, key string, sort ...string) error
	DBFindId(d string, c string, ret interface{}, id interface{}) (bool, error)
	// Find And Select
	DBSelectOne(d string, c string, ret interface{}, query interface{}, projection interface{}) (bool, error)
	DBSelectAll(d string, c string, ret interface{}, query interface{}, projection interface{}, sort ...string) error
	DBSelectRange(d string, c string, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error
	DBSelectPage(d string, c string, tot *uint32, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error
	DBSelectDistinct(d string, c string, ret interface{}, query interface{}, projection interface{}, key string, sort ...string) error
	DBSelectId(d string, c string, ret interface{}, id interface{}, projection interface{}) (bool, error)
	// FindAndModify
	DBFindAndUpdate(d string, c string, ret interface{}, query interface{}, update interface{}) (updated int, err error)              // return old doucument
	DBFindAndUpsert(d string, c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error)   // return old doucument
	DBFindAndRemove(d string, c string, ret interface{}, query interface{}) (removed int, err error)                                  // return old doucument
	DBFindAndUpdateRN(d string, c string, ret interface{}, query interface{}, update interface{}) (updated int, err error)            // return new doucument
	DBFindAndUpsertRN(d string, c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) // return new doucument

	DBInsert(d string, c string, docs ...interface{}) error
	DBRemoveOne(d string, c string, selector interface{}) (bool, error)
	DBRemoveAll(d string, c string, selector interface{}) (removed int, err error)
	DBRemoveId(d string, c string, id interface{}) (bool, error)
	DBUpdateOne(d string, c string, selector interface{}, update interface{}) (bool, error)
	DBUpdateAll(d string, c string, selector interface{}, update interface{}) (updated int, err error)
	DBUpdateId(d string, c string, id interface{}, update interface{}) (bool, error)
	DBUpsertOne(d string, c string, selector interface{}, update interface{}) (upsertedId interface{}, err error)
	DBUpsertId(d string, c string, id interface{}, update interface{}) (upsertedId interface{}, err error)
	DBRunBulk(d string, c string, f BulkFunc, args ...interface{}) (matched int, modified int, err error)
	DBRunCollection(d string, c string, f CollectionFunc, args ...interface{}) (interface{}, error)

	RunSession(f SessionFunc, args ...interface{}) (interface{}, error)
}

func Count(c string) (n int, err error) {
	return Default.Count(c)
}
func Indexes(c string) (indexes []mgo.Index, err error) {
	return Default.Indexes(c)
}
func EnsureIndex(c string, index mgo.Index) error {
	return Default.EnsureIndex(c, index)
}
func EnsureIndexKey(c string, key ...string) error {
	return Default.EnsureIndexKey(c, key...)
}
func DropIndex(c string, key ...string) error {
	return Default.DropIndex(c, key...)
}
func DropIndexName(c string, name string) error {
	return Default.DropIndexName(c, name)
}

func FindOne(c string, ret interface{}, query interface{}) (bool, error) {
	return Default.FindOne(c, query, ret)
}
func FindAll(c string, ret interface{}, query interface{}, sort ...string) error {
	return Default.FindAll(c, ret, query, sort...)
}
func FindRange(c string, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	return Default.FindRange(c, ret, query, skip, limit, sort...)
}

func FindPage(c string, tot *uint32, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	return Default.FindPage(c, tot, ret, query, skip, limit, sort...)
}

func FindDistinct(c string, ret interface{}, query interface{}, key string, sort ...string) error {
	return Default.FindDistinct(c, ret, query, key, sort...)
}
func FindId(c string, ret interface{}, id interface{}) (bool, error) {
	return Default.FindId(c, ret, id)
}

func SelectOne(c string, ret interface{}, query interface{}, projection interface{}) (bool, error) {
	return Default.SelectOne(c, ret, query, projection)
}
func SelectAll(c string, ret interface{}, query interface{}, projection interface{}, sort ...string) error {
	return Default.SelectAll(c, ret, query, projection, sort...)
}
func SelectRange(c string, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	return Default.SelectRange(c, ret, query, projection, skip, limit, sort...)
}
func SelectPage(c string, tot *uint32, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	return Default.SelectPage(c, tot, ret, query, projection, skip, limit, sort...)
}
func SelectDistinct(c string, ret interface{}, query interface{}, projection interface{}, key string, sort ...string) error {
	return Default.SelectDistinct(c, ret, query, projection, key, sort...)
}
func SelectId(c string, ret interface{}, id interface{}, projection interface{}) (bool, error) {
	return Default.SelectId(c, ret, id, projection)
}

func FindAndUpdate(c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	return Default.FindAndUpdate(c, ret, query, update)
}
func FindAndUpsert(c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	return Default.FindAndUpsert(c, ret, query, upsert)
}
func FindAndRemove(c string, ret interface{}, query interface{}) (removed int, err error) {
	return Default.FindAndRemove(c, ret, query)
}
func FindAndUpdateRN(c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	return Default.FindAndUpdateRN(c, ret, query, update)
}
func FindAndUpsertRN(c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	return Default.FindAndUpsertRN(c, ret, query, upsert)
}

func Insert(c string, docs ...interface{}) error {
	return Default.Insert(c, docs...)
}
func RemoveOne(c string, selector interface{}) (bool, error) {
	return Default.RemoveOne(c, selector)
}
func RemoveAll(c string, selector interface{}) (removed int, err error) {
	return Default.RemoveAll(c, selector)
}
func RemoveId(c string, id interface{}) (bool, error) {
	return Default.RemoveId(c, id)
}
func UpdateOne(c string, selector interface{}, update interface{}) (bool, error) {
	return Default.UpdateOne(c, selector, update)
}
func UpdateAll(c string, selector interface{}, update interface{}) (updated int, err error) {
	return Default.UpdateAll(c, selector, update)
}
func UpdateId(c string, id interface{}, update interface{}) (bool, error) {
	return Default.UpdateId(c, id, update)
}
func UpsertOne(c string, selector interface{}, update interface{}) (upsertedId interface{}, err error) {
	return Default.UpsertOne(c, selector, update)
}
func UpsertId(c string, id interface{}, update interface{}) (upsertedId interface{}, err error) {
	return Default.UpsertId(c, id, update)
}
func RunBulk(c string, f BulkFunc, args ...interface{}) (matched int, modified int, err error) {
	return Default.RunBulk(c, f, args...)
}

func RunCollection(c string, f CollectionFunc, args ...interface{}) (interface{}, error) {
	return Default.RunCollection(c, f, args...)
}

func DBCount(d string, c string) (n int, err error) {
	return Default.DBCount(d, c)
}
func DBIndexes(d string, c string) (indexes []mgo.Index, err error) {
	return Default.DBIndexes(d, c)
}
func DBEnsureIndex(d string, c string, index mgo.Index) error {
	return Default.DBEnsureIndex(d, c, index)
}
func DBEnsureIndexKey(d string, c string, key ...string) error {
	return Default.DBEnsureIndexKey(d, c, key...)
}
func DBDropIndex(d string, c string, key ...string) error {
	return Default.DBDropIndex(d, c, key...)
}
func DBDropIndexName(d string, c string, name string) error {
	return Default.DBDropIndexName(d, c, name)
}
func DBFindOne(d string, c string, ret interface{}, query interface{}) (bool, error) {
	return Default.DBFindOne(d, c, query, ret)
}
func DBFindAll(d string, c string, ret interface{}, query interface{}, sort ...string) error {
	return Default.DBFindAll(d, c, ret, query, sort...)
}
func DBFindRange(d string, c string, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	return Default.DBFindRange(d, c, ret, query, skip, limit, sort...)
}

func DBFindPage(d string, c string, tot *uint32, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	return Default.DBFindPage(d, c, tot, ret, query, skip, limit, sort...)
}

func DBFindDistinct(d string, c string, ret interface{}, query interface{}, key string, sort ...string) error {
	return Default.DBFindDistinct(d, c, ret, query, key, sort...)
}
func DBFindId(d string, c string, ret interface{}, id interface{}) (bool, error) {
	return Default.DBFindId(d, c, ret, id)
}

func DBSelectOne(d string, c string, ret interface{}, query interface{}, projection interface{}) (bool, error) {
	return Default.DBSelectOne(d, c, ret, query, projection)
}
func DBSelectAll(d string, c string, ret interface{}, query interface{}, projection interface{}, sort ...string) error {
	return Default.DBSelectAll(d, c, ret, query, projection, sort...)
}
func DBSelectRange(d string, c string, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	return Default.DBSelectRange(d, c, ret, query, projection, skip, limit, sort...)
}
func DBSelectPage(d string, c string, tot *uint32, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	return Default.DBSelectPage(d, c, tot, ret, query, projection, skip, limit, sort...)
}
func DBSelectDistinct(d string, c string, ret interface{}, query interface{}, projection interface{}, key string, sort ...string) error {
	return Default.DBSelectDistinct(d, c, ret, query, projection, key, sort...)
}
func DBSelectId(d string, c string, ret interface{}, id interface{}, projection interface{}) (bool, error) {
	return Default.DBSelectId(d, c, ret, id, projection)
}

func DBFindAndUpdate(d string, c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	return Default.DBFindAndUpdate(d, c, ret, query, update)
}
func DBFindAndUpsert(d string, c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	return Default.DBFindAndUpsert(d, c, ret, query, upsert)
}
func DBFindAndRemove(d string, c string, ret interface{}, query interface{}) (removed int, err error) {
	return Default.DBFindAndRemove(d, c, ret, query)
}
func DBFindAndUpdateRN(d string, c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	return Default.DBFindAndUpdateRN(d, c, ret, query, update)
}
func DBFindAndUpsertRN(d string, c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	return Default.DBFindAndUpsertRN(d, c, ret, query, upsert)
}

func DBInsert(d string, c string, docs ...interface{}) error {
	return Default.DBInsert(d, c, docs...)
}
func DBRemoveOne(d string, c string, selector interface{}) (bool, error) {
	return Default.DBRemoveOne(d, c, selector)
}
func DBRemoveAll(d string, c string, selector interface{}) (removed int, err error) {
	return Default.DBRemoveAll(d, c, selector)
}
func DBRemoveId(d string, c string, id interface{}) (bool, error) {
	return Default.DBRemoveId(d, c, id)
}
func DBUpdateOne(d string, c string, selector interface{}, update interface{}) (bool, error) {
	return Default.DBUpdateOne(d, c, selector, update)
}
func DBUpdateAll(d string, c string, selector interface{}, update interface{}) (updated int, err error) {
	return Default.DBUpdateAll(d, c, selector, update)
}
func DBUpdateId(d string, c string, id interface{}, update interface{}) (bool, error) {
	return Default.DBUpdateId(d, c, id, update)
}
func DBUpsertOne(d string, c string, selector interface{}, update interface{}) (upsertedId interface{}, err error) {
	return Default.DBUpsertOne(d, c, selector, update)
}
func DBUpsertId(d string, c string, id interface{}, update interface{}) (upsertedId interface{}, err error) {
	return Default.DBUpsertId(d, c, id, update)
}
func DBRunBulk(d string, c string, f BulkFunc, args ...interface{}) (matched int, modified int, err error) {
	return Default.DBRunBulk(d, c, f, args...)
}

func DBRunCollection(d string, c string, f CollectionFunc, args ...interface{}) (interface{}, error) {
	return Default.DBRunCollection(d, c, f, args...)
}

func RunSession(c string, f SessionFunc, args ...interface{}) (interface{}, error) {
	return Default.RunSession(f, args...)
}

type Config struct {
	// 连接URL, 格式为[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
	Address  []string
	Database string
	Username string
	Password string
	Source   string
	Safe     *mgo.Safe
	Mode     mgo.Mode

	// 连接管理
	ConnectTimeout time.Duration //连接超时. 默认为10秒
	Keepalive      time.Duration //DialInfo.DialServer实现
	WriteTimeout   time.Duration //写超时, 默认为ConnectTimeout
	ReadTimeout    time.Duration //读超时, 默认为ConnectTimeout

	// 连接池管理
	MinPoolSize       int //对应DialInfo.MinPoolSize
	MaxPoolSize       int //对应DialInfo.PoolLimit
	MaxPoolWaitTimeMS int //对应DialInfo.PoolTimeout获取连接超时, 默认为0永不超时
	MaxPoolIdleTimeMS int //对应DialInfo.MaxIdleTimeMS
}

var (
	Default Mongo
	Clients map[string]Mongo = make(map[string]Mongo)
)

func Get(name string) Mongo {
	if rt, ok := Clients[name]; ok {
		return rt
	}
	return nil
}

var (
	ErrDupKey = errors.New("duplicate client name")
)

func mergeOption(opt *Config) *Config {
	if opt == nil {
		opt = new(Config)
	}
	if opt.Safe == nil {
		opt.Safe = &mgo.Safe{
			WMode: Safe_majority,
			RMode: Safe_majority,
		}
	}
	return opt
}

func Setup(name string, opt *Config, def bool) (err error) {
	_, ok := Clients[name]
	if ok {
		err = ErrDupKey
		return
	}

	m, err := newGlobalsignMongo(mergeOption(opt))
	if err != nil {
		return
	}

	for _, k := range strings.Split(name, ",") {
		if k = strings.TrimSpace(k); len(k) > 0 {
			Clients[k] = m
		}
	}
	if def {
		Default = m
	}
	return
}

func GetMode(name string) mgo.Mode {
	switch name {
	case "Primary", "primary":
		return mgo.Primary
	case "PrimaryPreferred", "primaryPreferred":
		return mgo.PrimaryPreferred
	case "Secondary", "secondary":
		return mgo.Secondary
	case "SecondaryPreferred", "secondaryPreferred":
		return mgo.SecondaryPreferred
	case "Nearest", "nearest":
		return mgo.Nearest
	case "Eventual", "eventual":
		return mgo.Eventual
	case "Monotonic", "monotonic":
		return mgo.Monotonic
	case "Strong", "strong":
		return mgo.Strong
	}
	panic("Invalid mode name: " + name)
}

const (
	Safe_majority     = "majority"
	Safe_local        = "local"
	Safe_linearizable = "linearizable"
)
