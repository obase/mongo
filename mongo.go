package mongo

import (
	"errors"
	"github.com/globalsign/mgo"
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

func RunSession(c string, f SessionFunc, args ...interface{}) (interface{}, error) {
	return Default.RunSession(f, args...)
}

type Option struct {
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
	if name == "" {
		return Default
	}
	return Clients[name]
}

var (
	ErrDupKey = errors.New("duplicate client name")
)

func mergeOption(opt *Option) *Option {
	if opt == nil {
		opt = new(Option)
	}
	if opt.Safe == nil {
		opt.Safe = &mgo.Safe{
			WMode: Safe_majority,
			RMode: Safe_majority,
		}
	}
	return opt
}

func Setup(name string, opt *Option, def bool) (err error) {
	_, ok := Clients[name]
	if ok {
		err = ErrDupKey
		return
	}

	m, err := newGlobalsignMongo(mergeOption(opt))
	if err != nil {
		return
	}

	Clients[name] = m
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
