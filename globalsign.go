package mongo

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"net"
	"time"
)

var EMPTY_QUERY = bson.M{}

type gsBulk struct {
	*mgo.Bulk
}

func (b *gsBulk) Insert(docs ...interface{}) {
	b.Bulk.Insert(docs...)
}
func (b *gsBulk) Upsert(pairs ...interface{}) {
	b.Bulk.Upsert(pairs...)
}
func (b *gsBulk) RemoveOne(selectors ...interface{}) {
	b.Bulk.Remove(selectors...)
}
func (b *gsBulk) RemoveAll(selectors ...interface{}) {
	b.Bulk.RemoveAll(selectors...)
}
func (b *gsBulk) UpdateOne(pairs ...interface{}) {
	b.Bulk.Update(pairs...)
}
func (b *gsBulk) UpdateAll(pairs ...interface{}) {
	b.Bulk.UpdateAll(pairs...)
}

func (b *gsBulk) Run() (matched int, modified int, err error) {
	rs, err := b.Bulk.Run()
	if err != nil {
		return
	}
	return rs.Matched, rs.Modified, nil
}

type gsSession struct {
	*Config
	*mgo.Session
}

func (gs *gsSession) Count(c string) (n int, err error) {
	return gs.DBCount(gs.Config.Database, c)
}
func (gs *gsSession) Indexes(c string) (indexes []mgo.Index, err error) {
	return gs.DBIndexes(gs.Config.Database, c)
}
func (gs *gsSession) EnsureIndex(c string, index mgo.Index) (err error) {
	return gs.DBEnsureIndex(gs.Config.Database, c, index)
}
func (gs *gsSession) EnsureIndexKey(c string, key ...string) (err error) {
	return gs.DBEnsureIndexKey(gs.Config.Database, c, key...)
}
func (gs *gsSession) DropIndex(c string, key ...string) error {
	return gs.DBDropIndex(gs.Config.Database, c, key...)
}
func (gs *gsSession) DropIndexName(c string, name string) error {
	return gs.DBDropIndexName(gs.Config.Database, c, name)
}
func (gs *gsSession) FindOne(c string, ret interface{}, query interface{}) (ok bool, err error) {
	return gs.DBFindOne(gs.Config.Database, c, ret, query)
}
func (gs *gsSession) FindAll(c string, ret interface{}, query interface{}, sort ...string) error {
	return gs.DBFindAll(gs.Config.Database, c, ret, query, sort...)
}

func (gs *gsSession) FindRange(c string, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	return gs.DBFindRange(gs.Config.Database, c, ret, query, skip, limit, sort...)
}

func (gs *gsSession) FindPage(c string, tot *uint32, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	return gs.DBFindPage(gs.Config.Database, c, tot, ret, query, skip, limit, sort...)
}

func (gs *gsSession) FindDistinct(c string, ret interface{}, query interface{}, key string, sort ...string) error {
	return gs.DBFindDistinct(gs.Config.Database, c, ret, query, key, sort...)
}

func (gs *gsSession) FindId(c string, ret interface{}, id interface{}) (ok bool, err error) {
	return gs.DBFindId(gs.Config.Database, c, ret, id)
}

func (gs *gsSession) SelectOne(c string, ret interface{}, query interface{}, projection interface{}) (ok bool, err error) {
	return gs.DBSelectOne(gs.Config.Database, c, ret, query, projection)
}
func (gs *gsSession) SelectAll(c string, ret interface{}, query interface{}, projection interface{}, sort ...string) error {
	return gs.DBSelectAll(gs.Config.Database, c, ret, query, projection, sort...)
}
func (gs *gsSession) SelectRange(c string, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	return gs.DBSelectRange(gs.Config.Database, c, ret, query, projection, skip, limit, sort...)
}
func (gs *gsSession) SelectPage(c string, tot *uint32, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	return gs.DBSelectPage(gs.Config.Database, c, tot, ret, query, projection, skip, limit, sort...)
}
func (gs *gsSession) SelectDistinct(c string, ret interface{}, query interface{}, projection interface{}, key string, sort ...string) error {
	return gs.DBSelectDistinct(gs.Config.Database, c, ret, query, projection, key, sort...)
}
func (gs *gsSession) SelectId(c string, ret interface{}, id interface{}, projection interface{}) (ok bool, err error) {
	return gs.DBSelectId(gs.Config.Database, c, ret, id, projection)
}

func (gs *gsSession) FindAndUpdate(c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	return gs.DBFindAndUpdate(gs.Config.Database, c, ret, query, update)
}
func (gs *gsSession) FindAndUpsert(c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	return gs.DBFindAndUpsert(gs.Config.Database, c, ret, query, upsert)
}
func (gs *gsSession) FindAndRemove(c string, ret interface{}, query interface{}) (removed int, err error) {
	return gs.DBFindAndRemove(gs.Config.Database, c, ret, query)
}
func (gs *gsSession) FindAndUpdateRN(c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	return gs.DBFindAndUpdateRN(gs.Config.Database, c, ret, query, update)
}
func (gs *gsSession) FindAndUpsertRN(c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	return gs.DBFindAndUpsertRN(gs.Config.Database, c, ret, query, upsert)
}

func (gs *gsSession) Insert(c string, docs ...interface{}) (err error) {
	return gs.DBInsert(gs.Config.Database, c, docs...)
}
func (gs *gsSession) RemoveOne(c string, selector interface{}) (ok bool, err error) {
	return gs.DBRemoveOne(gs.Config.Database, c, selector)
}
func (gs *gsSession) RemoveAll(c string, selector interface{}) (removed int, err error) {
	return gs.DBRemoveAll(gs.Config.Database, c, selector)
}
func (gs *gsSession) RemoveId(c string, id interface{}) (ok bool, err error) {
	return gs.DBRemoveId(gs.Config.Database, c, id)
}
func (gs *gsSession) UpdateOne(c string, selector interface{}, update interface{}) (ok bool, err error) {
	return gs.DBUpdateOne(gs.Config.Database, c, selector, update)
}
func (gs *gsSession) UpdateAll(c string, selector interface{}, update interface{}) (updated int, err error) {
	return gs.DBUpdateAll(gs.Config.Database, c, selector, update)
}
func (gs *gsSession) UpdateId(c string, id interface{}, update interface{}) (ok bool, err error) {
	return gs.DBUpdateId(gs.Config.Database, c, id, update)
}
func (gs *gsSession) UpsertOne(c string, selector interface{}, update interface{}) (upsertId interface{}, err error) {
	return gs.DBUpsertOne(gs.Config.Database, c, selector, update)
}
func (gs *gsSession) UpsertId(c string, id interface{}, update interface{}) (upsertId interface{}, err error) {
	return gs.DBUpsertId(gs.Config.Database, c, id, update)
}
func (gs *gsSession) RunBulk(c string, f BulkFunc, args ...interface{}) (matched int, modified int, err error) {
	return gs.DBRunBulk(gs.Config.Database, c, f, args...)
}

func (gs *gsSession) RunCollection(c string, f CollectionFunc, args ...interface{}) (interface{}, error) {
	return gs.DBRunCollection(gs.Config.Database, c, f, args...)
}

func (gs *gsSession) DBCount(d string, c string) (n int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).Count()
}
func (gs *gsSession) DBIndexes(d string, c string) (indexes []mgo.Index, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).Indexes()
}
func (gs *gsSession) DBEnsureIndex(d string, c string, index mgo.Index) (err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).EnsureIndex(index)
}
func (gs *gsSession) DBEnsureIndexKey(d string, c string, key ...string) (err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).EnsureIndexKey(key...)
}
func (gs *gsSession) DBDropIndex(d string, c string, key ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).DropIndex(key...)
}
func (gs *gsSession) DBDropIndexName(d string, c string, name string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).DropIndexName(name)
}
func (gs *gsSession) DBFindOne(d string, c string, ret interface{}, query interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	err = cs.DB(d).C(c).Find(query).One(ret)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			ok = false
		} else {
			return
		}
	} else {
		ok = true
	}
	return
}
func (gs *gsSession) DBFindAll(d string, c string, ret interface{}, query interface{}, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(d).C(c).Find(query)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	return q.All(ret)
}

func (gs *gsSession) DBFindRange(d string, c string, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(d).C(c).Find(query)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	if skip > 0 {
		q.Skip(int(skip))
	}
	if limit > 0 {
		q.Limit(int(limit))
	}
	return q.All(ret)
}

func (gs *gsSession) DBFindPage(d string, c string, tot *uint32, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(d).C(c).Find(query)
	t, err := q.Count()
	if err != nil {
		return err
	}
	*tot = uint32(t)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	if skip > 0 {
		q.Skip(int(skip))
	}
	if limit > 0 {
		q.Limit(int(limit))
	}
	return q.All(ret)
}

func (gs *gsSession) DBFindDistinct(d string, c string, ret interface{}, query interface{}, key string, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(d).C(c).Find(query)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	return q.Distinct(key, ret)
}

func (gs *gsSession) DBFindId(d string, c string, ret interface{}, id interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(d).C(c).FindId(id).One(ret)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			ok = false
		} else {
			return
		}
	} else {
		ok = true
	}
	return
}

func (gs *gsSession) DBSelectOne(d string, c string, ret interface{}, query interface{}, projection interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	err = cs.DB(d).C(c).Find(query).Select(projection).One(ret)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			ok = false
		} else {
			return
		}
	} else {
		ok = true
	}
	return
}
func (gs *gsSession) DBSelectAll(d string, c string, ret interface{}, query interface{}, projection interface{}, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(d).C(c).Find(query).Select(projection)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	return q.All(ret)
}
func (gs *gsSession) DBSelectRange(d string, c string, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(d).C(c).Find(query).Select(projection)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	if skip > 0 {
		q.Skip(int(skip))
	}
	if limit > 0 {
		q.Limit(int(limit))
	}
	return q.All(ret)
}
func (gs *gsSession) DBSelectPage(d string, c string, tot *uint32, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	if projection == nil {
		projection = EMPTY_QUERY
	}
	q := cs.DB(d).C(c).Find(query).Select(projection)
	t, err := q.Count()
	if err != nil {
		return err
	}
	*tot = uint32(t)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	if skip > 0 {
		q.Skip(int(skip))
	}
	if limit > 0 {
		q.Limit(int(limit))
	}
	return q.All(ret)
}
func (gs *gsSession) DBSelectDistinct(d string, c string, ret interface{}, query interface{}, projection interface{}, key string, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	if projection == nil {
		projection = EMPTY_QUERY
	}
	q := cs.DB(d).C(c).Find(query).Select(projection)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	return q.Distinct(key, ret)
}
func (gs *gsSession) DBSelectId(d string, c string, ret interface{}, id interface{}, projection interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(d).C(c).FindId(id).Select(projection).One(ret)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			ok = false
		} else {
			return
		}
	} else {
		ok = true
	}
	return
}

func (gs *gsSession) DBFindAndUpdate(d string, c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(d).C(c).Find(query).Apply(mgo.Change{
		Update:    update,
		Upsert:    false,
		Remove:    false,
		ReturnNew: false,
	}, ret)
	if err != nil {
		return
	}
	updated = ci.Updated
	return
}
func (gs *gsSession) DBFindAndUpsert(d string, c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(d).C(c).Find(query).Apply(mgo.Change{
		Update:    upsert,
		Upsert:    true,
		Remove:    false,
		ReturnNew: false,
	}, ret)
	if err != nil {
		return
	}
	upsertedId = ci.UpsertedId
	return
}
func (gs *gsSession) DBFindAndRemove(d string, c string, ret interface{}, query interface{}) (removed int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(d).C(c).Find(query).Apply(mgo.Change{
		Update:    nil,
		Upsert:    false,
		Remove:    true,
		ReturnNew: false,
	}, ret)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
		}
		return
	}
	removed = ci.Removed
	return
}
func (gs *gsSession) DBFindAndUpdateRN(d string, c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(d).C(c).Find(query).Apply(mgo.Change{
		Update:    update,
		Upsert:    false,
		Remove:    false,
		ReturnNew: true,
	}, ret)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
		}
		return
	}
	updated = ci.Updated
	return
}
func (gs *gsSession) DBFindAndUpsertRN(d string, c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(d).C(c).Find(query).Apply(mgo.Change{
		Update:    upsert,
		Upsert:    true,
		Remove:    false,
		ReturnNew: true,
	}, ret)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
		}
		return
	}
	upsertedId = ci.UpsertedId
	return
}

func (gs *gsSession) DBInsert(d string, c string, docs ...interface{}) (err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).Insert(docs...)
}
func (gs *gsSession) DBRemoveOne(d string, c string, selector interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(d).C(c).Remove(selector)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			ok = false
		} else {
			return
		}
	} else {
		ok = true
	}
	return
}
func (gs *gsSession) DBRemoveAll(d string, c string, selector interface{}) (removed int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	ci, err := cs.DB(d).C(c).RemoveAll(selector)
	if err != nil {
		return
	}
	removed = ci.Removed
	return
}
func (gs *gsSession) DBRemoveId(d string, c string, id interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(d).C(c).RemoveId(id)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			ok = false
		} else {
			return
		}
	} else {
		ok = true
	}
	return
}
func (gs *gsSession) DBUpdateOne(d string, c string, selector interface{}, update interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(d).C(c).Update(selector, update)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			ok = false
		} else {
			return
		}
	} else {
		ok = true
	}
	return
}
func (gs *gsSession) DBUpdateAll(d string, c string, selector interface{}, update interface{}) (updated int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	ci, err := cs.DB(d).C(c).UpdateAll(selector, update)
	if err != nil {
		return
	}
	updated = ci.Updated
	return
}
func (gs *gsSession) DBUpdateId(d string, c string, id interface{}, update interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(d).C(c).UpdateId(id, update)
	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			ok = false
		} else {
			return
		}
	} else {
		ok = true
	}
	return
}
func (gs *gsSession) DBUpsertOne(d string, c string, selector interface{}, update interface{}) (upsertId interface{}, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).Upsert(selector, update)
}
func (gs *gsSession) DBUpsertId(d string, c string, id interface{}, update interface{}) (upsertId interface{}, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(d).C(c).UpsertId(id, update)
}
func (gs *gsSession) DBRunBulk(d string, c string, f BulkFunc, args ...interface{}) (matched int, modified int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	bk := cs.DB(d).C(c).Bulk()
	f(&gsBulk{Bulk: bk}, args...)
	rs, err := bk.Run()
	if rs != nil && err == nil {
		matched = rs.Matched
		modified = rs.Modified
	}
	return
}

func (gs *gsSession) DBRunCollection(d string, c string, f CollectionFunc, args ...interface{}) (interface{}, error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	cl := cs.DB(d).C(c)
	return f(cl, args...)
}

func (gs *gsSession) RunSession(f SessionFunc, args ...interface{}) (interface{}, error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return f(cs, args...)
}

func newGlobalsignMongo(opt *Config) (*gsSession, error) {
	dsf := func(addr *mgo.ServerAddr) (net.Conn, error) {
		tcp, err := net.DialTCP("tcp", nil, addr.TCPAddr())
		if err != nil {
			return nil, err
		}
		if opt.Keepalive > 0 {
			tcp.SetKeepAlive(true)
			tcp.SetKeepAlivePeriod(opt.Keepalive)
		}
		return tcp, nil
	}
	di := &mgo.DialInfo{
		Addrs:    opt.Address,
		Database: opt.Database,
		Username: opt.Username,
		Password: opt.Password,
		Source:   opt.Source,

		Timeout:      opt.ConnectTimeout,
		DialServer:   dsf,
		WriteTimeout: opt.WriteTimeout,
		ReadTimeout:  opt.ReadTimeout,

		MinPoolSize:   opt.MinPoolSize,
		PoolLimit:     opt.MaxPoolSize,
		PoolTimeout:   time.Duration(opt.MaxPoolWaitTimeMS) * time.Millisecond,
		MaxIdleTimeMS: opt.MaxPoolIdleTimeMS,
	}
	ms, err := mgo.DialWithInfo(di)
	if err != nil {
		return nil, err
	}
	ms.SetSafe(opt.Safe)       //数据安全. 参考https://godoc.org/github.com/globalsign/mgo#Safe
	ms.SetMode(opt.Mode, true) // 读写时序. 参考https://docs.mongodb.com/manual/reference/read-preference/
	return &gsSession{Session: ms, Config: opt}, nil
}
