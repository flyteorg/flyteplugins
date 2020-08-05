package awsbatch

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateSerialization(t *testing.T) {
	//s := State{
	//	ExternalJobID: ref("test"),
	//}
	//
	//w := bytes.Buffer{}
	//if assert.NoError(t, gob.NewEncoder(&w).Encode(&s)) {
	//	t.Log(w.String())
	//}
	//
	//strMarshaled, err := yaml.Marshal(w.String())
	//if assert.NoError(t, err) {
	//	t.Log(string(strMarshaled))
	//}
	//
	//unSt := State{}
	//assert.NoError(t, gob.NewDecoder(&w).Decode(&unSt))

	str := `I/+DAwEBC1BsdWdpblN0YXRlAf+EAAEBAQVQaGFzZQEGAAAABf+EAQIA`
	reader := base64.NewDecoder(base64.StdEncoding, bytes.NewReader([]byte(str)))
	gob.Register(State{})
	b, er := ioutil.ReadAll(reader)
	if assert.NoError(t, er) {
		t.Log(string(b))
	}
	dec := gob.NewDecoder(reader)
	st := State{}
	err := dec.Decode(&st)
	if assert.NoError(t, err) {
		t.Logf("Deserialized State: [%+v]", st)
	}

	t.FailNow()
}
