package nativebootstrap

import "testing"

func TestClosedObjectRejectsAmbiguousKeys(t *testing.T) {
	for _, body := range []string{`{"ticket":"a","ticket":"b"}`, `{"ticket":"a","Ticket":"b"}`, `{"Ticket":"a"}`, `{"unknown":"a"}`, `{"ticket":"a"} {}`, `[]`, `null`, `{"ticket":{}}`, `{"ticket":"a"`} {
		t.Run(body, func(t *testing.T) {
			var req struct {
				Ticket string `json:"ticket"`
			}
			if DecodeObject([]byte(body), &req) == nil {
				t.Fatal("accepted ambiguous or invalid closed object")
			}
		})
	}
	var req struct {
		Ticket string `json:"ticket"`
	}
	if err := DecodeObject([]byte(`{"ticket":"a"}`), &req); err != nil || req.Ticket != "a" {
		t.Fatal("rejected canonical object", err)
	}
}
