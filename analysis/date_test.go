package analysis

import (
	"testing"
	"time"
	"fmt"
)

func TestGroup(t *testing.T) {
	now := time.Now()
	var testData Series
	for i:=0; i < 1000; i++ {
		sample := Sample{10., now.Add(time.Second*time.Duration(i))}
		testData = append(testData, sample)
	}
	d, _ := time.ParseDuration("5s")
	grouped := testData.GroupByTime(d)
	if len(grouped) != 200 {
		t.Logf("length should be 200 but is %d", len(grouped))
		t.Fail()
	}
	for _, g := range grouped {
		if len(g) != 5 {
			t.Logf("length should be 5 but is %d", len(g))
			t.Fail()
		}
	}
	fmt.Println(testData.SMA(10))
	fmt.Println(testData.EMA(10))
}