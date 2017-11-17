package crawler

import (
	"encoding/json"
	"testing"
)

func TestCustomTime(t *testing.T) {
	ts := "\"2017-11-16 06:02:20\""
	ct := CustomTime{}
	err := json.Unmarshal([]byte(ts), &ct)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(ct)
}
