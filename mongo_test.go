package mongo

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"testing"
)

const (
	c = "jx3robot"
)

func TestCount(t *testing.T) {
	fmt.Println(Count(c))
}

func TestFindOne(t *testing.T) {
	var m map[string]interface{} = make(map[string]interface{})
	fmt.Println(FindId(c, &m, 1))
	fmt.Println(m)
}

func TestFindAll(t *testing.T) {
	var ms []map[string]interface{}
	fmt.Println(FindAll(c, &ms, nil))
	fmt.Println(ms)
}

func TestRunBulk(t *testing.T) {
	RunBulk(c, func(bk Bulk, args ...interface{}) {
		bk.Insert(bson.M{"_id": 43, "name": "hehe"})
		bk.UpdateOne(bson.M{"_id": 43}, bson.M{"name": "hehe"})
	})
}

func TestUpsertOne(t *testing.T) {
	fmt.Println(RemoveId(c, 3))
}

func TestGsSession_FindAndUpsert(t *testing.T) {
	var m map[string]interface{}
	fmt.Println(FindAndRemove(c, &m, bson.M{"_id": "test1"}))
	fmt.Println(m)
}

func TestFindPage(t *testing.T) {
	var tot uint32
	var ms []map[string]interface{}
	fmt.Println(FindPage(c, &tot, &ms, nil, 0, 3, "-value"))
	fmt.Println(tot)
	fmt.Println(ms)
}

func TestSelectAll(t *testing.T) {
	var ms []map[string]interface{}
	fmt.Println(SelectAll(c, &ms, nil, bson.M{"$or": 1}))
	fmt.Println(ms)
}
