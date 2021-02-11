package hutch

import (
	"encoding/json"
	"fmt"
)

type DeliverableMessage interface {
	RawMarshal
}

type EncodableMessage interface {
	Encode() ([]byte, error)
	ContentType() string
}

type RawMessage interface {
	EncodableMessage
	Data() map[string]interface{}
	Kind() string
}

type rawMessage struct {
	Meta        map[string]interface{} `json:"meta"`
	MessageData map[string]interface{} `json:"data"`
	raw         []byte
	kind        string
	id          string
}

type RawMarshal interface {
	MarshalRaw() (RawMessage, error)
}

func NewRawMessage(meta, data map[string]interface{}, kind string) RawMessage {
	return &rawMessage{
		Meta:        meta,
		MessageData: data,
		raw:         make([]byte, 0),
		kind:        kind,
	}
}

const (
	ContentApplicationJSON = "application/json"
)

type MessageMeta interface {
	MetaMarshaller
	Kind() string
}

type messageMeta struct {
	ID      string
	kind    string
	Version string
}

func NewMessageMeta(id, kind, version string) MessageMeta {
	return &messageMeta{
		ID:      id,
		kind:    kind,
		Version: version,
	}
}

func (m messageMeta) MarshalMeta() (map[string]interface{}, error) {
	meta := make(map[string]interface{})
	meta["id"] = m.ID
	meta["kind"] = m.Kind()
	meta["version"] = m.Version
	return meta, nil
}

func (m messageMeta) Kind() string {
	return m.kind
}

type DataMarshaller interface {
	MarshalData() (map[string]interface{}, error)
}

type MetaMarshaller interface {
	MarshalMeta() (map[string]interface{}, error)
}

type MessageData interface {
	DataMarshaller
}

type Message struct {
	Meta MessageMeta
	Data MessageData
}

func (m Message) MarshalRaw() (RawMessage, error) {
	meta, err := m.Meta.MarshalMeta()
	if err != nil {
		return nil, err
	}
	data, err := m.Data.MarshalData()
	if err != nil {
		return nil, err
	}
	r := NewRawMessage(meta, data, m.Meta.Kind())

	return r, nil
}

func (r rawMessage) Encode() ([]byte, error) {
	msg, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (r rawMessage) ContentType() string {
	return ContentApplicationJSON
}

func (r rawMessage) Kind() string {
	return r.kind
}

func (r rawMessage) Data() map[string]interface{} {
	return r.MessageData
}

// JSONDataMarshal uses the encoding/json marshal and unmarshal methods to turn the data
// into a map[string]interface{} this allows using json tags on an object to format the
// marshalled and unmarshalled data
func JSONDataMarshal(d interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(d)
	if err != nil {
		fmt.Printf("Failed to marshal data: %s\n", err)
		return nil, err
	}
	dat := make(map[string]interface{})
	if err := json.Unmarshal(b, &dat); err != nil {
		fmt.Printf("Failed to marshal data: %s\n", err)
		return nil, err
	}
	return dat, nil
}

func JSONDataUnmarshal(d map[string]interface{}, v interface{}) error {
	b, err := json.Marshal(d)
	if err != nil {
		fmt.Printf("Failed to marshal data: %s\n", err)
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		fmt.Printf("Failed to marshal data: %s\n", err)
		return err
	}
	return nil
}

func RawUnmarshal(msg []byte, v interface{}) error {
	err := json.Unmarshal(msg, v)
	if err != nil {
		return err
	}
	return nil
}
