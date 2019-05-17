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
	*Option
	*mgo.Session
}

func (gs *gsSession) Count(c string) (n int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(gs.Option.Database).C(c).Count()
}
func (gs *gsSession) Indexes(c string) (indexes []mgo.Index, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(gs.Option.Database).C(c).Indexes()
}
func (gs *gsSession) EnsureIndex(c string, index mgo.Index) (err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(gs.Option.Database).C(c).EnsureIndex(index)
}
func (gs *gsSession) EnsureIndexKey(c string, key ...string) (err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(gs.Option.Database).C(c).EnsureIndexKey(key...)
}
func (gs *gsSession) FindOne(c string, ret interface{}, query interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	err = cs.DB(gs.Option.Database).C(c).Find(query).One(ret)
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
func (gs *gsSession) FindAll(c string, ret interface{}, query interface{}, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(gs.Option.Database).C(c).Find(query)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	return q.All(ret)
}

func (gs *gsSession) FindRange(c string, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(gs.Option.Database).C(c).Find(query)
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

func (gs *gsSession) FindPage(c string, tot *uint32, ret interface{}, query interface{}, skip uint32, limit uint32, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(gs.Option.Database).C(c).Find(query)
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

func (gs *gsSession) FindDistinct(c string, ret interface{}, query interface{}, key string, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(gs.Option.Database).C(c).Find(query)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	return q.Distinct(key, ret)
}

func (gs *gsSession) FindId(c string, ret interface{}, id interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(gs.Option.Database).C(c).FindId(id).One(ret)
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

func (gs *gsSession) SelectOne(c string, ret interface{}, query interface{}, projection interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	err = cs.DB(gs.Option.Database).C(c).Find(query).Select(projection).One(ret)
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
func (gs *gsSession) SelectAll(c string, ret interface{}, query interface{}, projection interface{}, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(gs.Option.Database).C(c).Find(query).Select(projection)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	return q.All(ret)
}
func (gs *gsSession) SelectRange(c string, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	q := cs.DB(gs.Option.Database).C(c).Find(query).Select(projection)
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
func (gs *gsSession) SelectPage(c string, tot *uint32, ret interface{}, query interface{}, projection interface{}, skip uint32, limit uint32, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	if projection == nil {
		projection = EMPTY_QUERY
	}
	q := cs.DB(gs.Option.Database).C(c).Find(query).Select(projection)
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
func (gs *gsSession) SelectDistinct(c string, ret interface{}, query interface{}, projection interface{}, key string, sort ...string) error {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	if projection == nil {
		projection = EMPTY_QUERY
	}
	q := cs.DB(gs.Option.Database).C(c).Find(query).Select(projection)
	if len(sort) > 0 {
		q.Sort(sort...)
	}
	return q.Distinct(key, ret)
}
func (gs *gsSession) SelectId(c string, ret interface{}, id interface{}, projection interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(gs.Option.Database).C(c).FindId(id).Select(projection).One(ret)
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

func (gs *gsSession) FindAndUpdate(c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(gs.Option.Database).C(c).Find(query).Apply(mgo.Change{
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
func (gs *gsSession) FindAndUpsert(c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(gs.Option.Database).C(c).Find(query).Apply(mgo.Change{
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
func (gs *gsSession) FindAndRemove(c string, ret interface{}, query interface{}) (removed int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(gs.Option.Database).C(c).Find(query).Apply(mgo.Change{
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
func (gs *gsSession) FindAndUpdateRN(c string, ret interface{}, query interface{}, update interface{}) (updated int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(gs.Option.Database).C(c).Find(query).Apply(mgo.Change{
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
func (gs *gsSession) FindAndUpsertRN(c string, ret interface{}, query interface{}, upsert interface{}) (upsertedId interface{}, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	if query == nil {
		query = EMPTY_QUERY
	}
	ci, err := cs.DB(gs.Option.Database).C(c).Find(query).Apply(mgo.Change{
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

func (gs *gsSession) Insert(c string, docs ...interface{}) (err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(gs.Option.Database).C(c).Insert(docs...)
}
func (gs *gsSession) RemoveOne(c string, selector interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(gs.Option.Database).C(c).Remove(selector)
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
func (gs *gsSession) RemoveAll(c string, selector interface{}) (removed int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	ci, err := cs.DB(gs.Option.Database).C(c).RemoveAll(selector)
	if err != nil {
		return
	}
	removed = ci.Removed
	return
}
func (gs *gsSession) RemoveId(c string, id interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(gs.Option.Database).C(c).RemoveId(id)
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
func (gs *gsSession) UpdateOne(c string, selector interface{}, update interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(gs.Option.Database).C(c).Update(selector, update)
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
func (gs *gsSession) UpdateAll(c string, selector interface{}, update interface{}) (updated int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	ci, err := cs.DB(gs.Option.Database).C(c).UpdateAll(selector, update)
	if err != nil {
		return
	}
	updated = ci.Updated
	return
}
func (gs *gsSession) UpdateId(c string, id interface{}, update interface{}) (ok bool, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	err = cs.DB(gs.Option.Database).C(c).UpdateId(id, update)
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
func (gs *gsSession) UpsertOne(c string, selector interface{}, update interface{}) (upsertId interface{}, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(gs.Option.Database).C(c).Upsert(selector, update)
}
func (gs *gsSession) UpsertId(c string, id interface{}, update interface{}) (upsertId interface{}, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return cs.DB(gs.Option.Database).C(c).UpsertId(id, update)
}
func (gs *gsSession) RunBulk(c string, f BulkFunc, args ...interface{}) (matched int, modified int, err error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	bk := cs.DB(gs.Option.Database).C(c).Bulk()
	f(&gsBulk{Bulk: bk}, args...)
	rs, err := bk.Run()
	matched = rs.Matched
	modified = rs.Modified
	return
}

func (gs *gsSession) RunCollection(c string, f CollectionFunc, args ...interface{}) (interface{}, error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	cl := cs.DB(gs.Option.Database).C(c)
	return f(cl, args...)
}

func (gs *gsSession) RunSession(f SessionFunc, args ...interface{}) (interface{}, error) {
	cs := gs.Session.Copy()
	defer cs.Close()

	return f(cs, args...)
}

func newGlobalsignMongo(opt *Option) (*gsSession, error) {
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
	return &gsSession{Session: ms, Option: opt}, nil
}
