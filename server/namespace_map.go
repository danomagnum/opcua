package server

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gopcua/opcua/debug"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/server/attrs"
	"github.com/gopcua/opcua/ua"
)

// This namespaces give a convenient way to have data mapped to the OPC server
// without having to map your application data to the OCP-UA data abstraction
//
// It (currently) supports ints, floats, strings, and timestamps. No maps inside of maps and no arrays.
//
// To notify subscribers of changes, be sure to call ChangeNotification(key) after changing the value.
// To be notified of changes from the opc-ua server to the map, receive on ExternalNotification channel
type MapNamespace struct {
	srv  *Server
	name string
	Mu   sync.RWMutex
	Data map[string]any

	// This can be used to be alerted when a value is changed from the opc server
	ExternalNotification chan string

	id uint16
}

// Get the value associated with key from the MapNamespace.
// This function handles locking and getting the value.
//
// Returns nil if the value doesn't exist.
func (s *MapNamespace) GetValue(key string) any {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Data[key]
}

// update the value associated with a key and trigger the change notification
// to the OPC server
func (s *MapNamespace) SetValue(key string, value any) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Data[key] = value
	s.ChangeNotification(key)
}

// This function is used to notify OPC UA subscribers if a key was changed without using the
// SetValue() function
func (s *MapNamespace) ChangeNotification(key string) {
	s.srv.ChangeNotification(ua.NewStringNodeID(s.id, key))
}

func NewMapNamespace(srv *Server, name string) *MapNamespace {
	mrw := MapNamespace{
		srv:                  srv,
		name:                 name,
		Data:                 make(map[string]any),
		ExternalNotification: make(chan string),
	}
	return &mrw
}

func (s *MapNamespace) ID() uint16 {
	return s.id
}
func (ns *MapNamespace) SetID(id uint16) {
	ns.id = id
}

func (ns *MapNamespace) Browse(bd *ua.BrowseDescription) *ua.BrowseResult {
	ns.Mu.RLock()
	defer ns.Mu.RUnlock()

	debug.Printf("BrowseRequest: id=%s mask=%08b\n", bd.NodeID, bd.ResultMask)
	//
	log.Printf("Browse req for %s", bd.NodeID.String())
	if bd.NodeID.IntID() != id.RootFolder && bd.NodeID.IntID() != id.ObjectsFolder {

		return &ua.BrowseResult{StatusCode: ua.StatusBadNodeIDUnknown}
	}

	if bd.NodeID.IntID() == id.RootFolder {

		refs := make([]*ua.ReferenceDescription, 1)
		newid := ua.NewNumericNodeID(ns.id, id.ObjectsFolder)
		expnewid := ua.NewNumericExpandedNodeID(ns.id, id.ObjectsFolder)
		refs[0] = &ua.ReferenceDescription{
			ReferenceTypeID: newid,
			NodeID:          expnewid,
			BrowseName:      &ua.QualifiedName{NamespaceIndex: ns.id, Name: "objects"},
			DisplayName:     &ua.LocalizedText{Text: "objects"},
			TypeDefinition:  expnewid,
		}

		return &ua.BrowseResult{
			StatusCode: ua.StatusGood,
			References: refs,
		}

	}

	refs := make([]*ua.ReferenceDescription, len(ns.Data))

	keyid := 0
	for k := range ns.Data {
		key := k
		newid := ua.NewStringNodeID(ns.id, key)
		expnewid := ua.NewStringExpandedNodeID(ns.id, key)

		refs[keyid] = &ua.ReferenceDescription{
			ReferenceTypeID: newid,
			IsForward:       true,
			NodeID:          expnewid,
			BrowseName:      &ua.QualifiedName{NamespaceIndex: ns.ID(), Name: key},
			DisplayName:     &ua.LocalizedText{EncodingMask: ua.LocalizedTextText, Text: key},
			NodeClass:       ua.NodeClassVariable, // when support is added for nested maps, this will be NodeClassObject
			TypeDefinition:  expnewid,
		}
		keyid++
	}

	return &ua.BrowseResult{
		StatusCode: ua.StatusGood,
		References: refs,
	}

}

func (ns *MapNamespace) Attribute(n *ua.NodeID, a ua.AttributeID) *ua.DataValue {
	log.Printf("read: node=%s attr=%s", n.String(), a)

	if n.IntID() != 0 {
		// this is not one of our normal tags.
		if n.IntID() != id.ObjectsFolder {
			return &ua.DataValue{
				EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
				ServerTimestamp: time.Now(),
				Status:          ua.StatusBadNodeIDInvalid,
			}
		}

		attrval, err := ns.Objects().Attribute(a)
		if err != nil {
			return &ua.DataValue{
				EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
				ServerTimestamp: time.Now(),
				Status:          ua.StatusBadAttributeIDInvalid,
			}
		}

		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode | ua.DataValueValue,
			ServerTimestamp: time.Now(),
			Status:          ua.StatusOK,
			Value:           attrval.Value,
		}

	}

	dv := &ua.DataValue{
		EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
		ServerTimestamp: time.Now(),
		Status:          ua.StatusBad,
	}

	key := strip_crap(n.String())
	log.Printf("Read req for %s", key)
	var err error
	log.Printf("'%s' Data at read: %v", ns.name, ns.Data)

	// we are going to use the node id directly to look it up from our data map.
	if a == ua.AttributeIDValue {
		dv.Status = ua.StatusOK
		dv.EncodingMask |= ua.DataValueValue
		v, ok := ns.Data[key]
		if !ok {
			return &ua.DataValue{
				EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
				ServerTimestamp: time.Now(),
				Status:          ua.StatusBadNodeIDUnknown,
			}
		}
		switch tv := v.(type) {
		case string:
			dv.Value = ua.MustVariant(tv)
		case int:
			// we can't use an int because it is of unspecified length.  I'm going to use int64 so that we don't
			// have to worry about cutting data off. probably.
			dv.Value = ua.MustVariant(int64(tv))
		case int32:
			dv.Value = ua.MustVariant(tv)
		case float32:
			dv.Value = ua.MustVariant(tv)
		case float64:
			dv.Value = ua.MustVariant(tv)
		case bool:
			dv.Value = ua.MustVariant(tv)
		default:
			dv.Value = ua.MustVariant(tv)
		}
	}
	// nothing in this namespace has an ID Description
	if a == ua.AttributeIDDescription {
		dv.Status = ua.StatusOK
		dv.EncodingMask |= ua.DataValueValue
		dv.Value = ua.MustVariant("")
	}
	// nothing in this namespace has event notifiers
	if a == ua.AttributeIDEventNotifier {
		dv.Status = ua.StatusOK
		dv.EncodingMask |= ua.DataValueValue
		dv.Value = ua.MustVariant(int16(0))
	}

	// values are in section 5.1.2 of the standard.
	// https://reference.opcfoundation.org/Core/Part6/v104/docs/5.1.2
	if a == ua.AttributeIDDataType {
		dv.Status = ua.StatusOK
		dv.EncodingMask |= ua.DataValueValue
		v := ns.Data[key]
		switch v.(type) {
		case string:
			dv.Value, err = ua.NewVariant(ua.NewNumericNodeID(0, 12))
			if err != nil {
				log.Printf("problem creating variant: %v", err)
			}
		case int:
			// we can't use an int because it is of unspecified length.  I'm going to use int64 so that we don't
			// have to worry about cutting data off.
			dv.Value, err = ua.NewVariant(ua.NewNumericNodeID(0, 6))
			if err != nil {
				log.Printf("problem creating variant: %v", err)
			}
		case int32:
			dv.Value, err = ua.NewVariant(ua.NewNumericNodeID(0, 6))
			if err != nil {
				log.Printf("problem creating variant: %v", err)
			}
		case float32:
			dv.Value, err = ua.NewVariant(ua.NewNumericNodeID(0, 10))
			if err != nil {
				log.Printf("problem creating variant: %v", err)
			}
		case float64:
			dv.Value, err = ua.NewVariant(ua.NewNumericNodeID(0, 11))
			if err != nil {
				log.Printf("problem creating variant: %v", err)
			}
		case bool:
			dv.Value, err = ua.NewVariant(ua.NewNumericNodeID(0, 1))
			if err != nil {
				log.Printf("problem creating variant: %v", err)
			}
		default:
			dv.Value, err = ua.NewVariant(ua.NewNumericNodeID(0, 24))
			if err != nil {
				log.Printf("problem creating variant: %v", err)
			}
		}
	}

	// when we support arrays this will have to change.
	if a == ua.AttributeIDValueRank {
		dv.Status = ua.StatusOK
		dv.EncodingMask |= ua.DataValueValue
		dv.Value = ua.MustVariant(int32(-1))
	}

	// when we support arrays this will have to change.
	if a == ua.AttributeIDArrayDimensions {
		dv.Status = ua.StatusOK
		dv.EncodingMask |= ua.DataValueValue
		dv.Value = ua.MustVariant([]uint32{})
	}

	if dv.Value == nil {
		log.Printf("bad dv.Value!")
	}
	debug.Printf("Read '%s' = '%v' (%v)", key, dv.Value, dv.Value.Value())

	return dv
}

func (s *MapNamespace) SetAttribute(node *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {

	s.Mu.Lock()
	defer s.Mu.Unlock()
	log.Printf("'%s' Data pre-write: %v", s.name, s.Data)

	key := strip_crap(node.StringID())

	// we would normally look up the node in our actual address space, but since that's dumb, we're just
	// going to use the node id directly to look it up from our data map.
	if attr == ua.AttributeIDValue {
		v := val.Value.Value()
		s.Data[key] = v
	}

	// notify the opc ua server the value has changed.
	s.srv.ChangeNotification(node)
	// notify the non-opc application the value has changed.
	select {
	case s.ExternalNotification <- key:
	default:
	}

	return ua.StatusOK
}

func strip_crap(s string) string {
	seq_pos := strings.LastIndex(s, "s=")
	if seq_pos < 0 {
		return s
	}
	return s[seq_pos+2:]
}

func (ns *MapNamespace) Name() string {
	return ns.name
}
func (ns *MapNamespace) AddNode(n *Node) *Node {
	return n
}
func (ns *MapNamespace) Node(id *ua.NodeID) *Node {
	return nil

}
func (ns *MapNamespace) Objects() *Node {
	oid := ua.NewNumericNodeID(ns.ID(), id.ObjectsFolder)
	//eoid := ua.NewNumericExpandedNodeID(ns.ID(), id.ObjectsFolder)
	typedef := ua.NewNumericExpandedNodeID(0, id.ObjectsFolder)
	//reftype := ua.NewTwoByteNodeID(uint8(id.HasComponent)) // folder
	n := NewNode(
		oid,
		map[ua.AttributeID]*ua.Variant{
			ua.AttributeIDNodeClass:     ua.MustVariant(uint32(ua.NodeClassObject)),
			ua.AttributeIDBrowseName:    ua.MustVariant(attrs.BrowseName(ns.name)),
			ua.AttributeIDDisplayName:   ua.MustVariant(attrs.DisplayName(ns.name, ns.name)),
			ua.AttributeIDDescription:   ua.MustVariant(uint32(ua.NodeClassObject)),
			ua.AttributeIDDataType:      ua.MustVariant(typedef),
			ua.AttributeIDEventNotifier: ua.MustVariant(int16(0)),
		},
		[]*ua.ReferenceDescription{},
		nil,
	)
	return n

}
func (ns *MapNamespace) Root() *Node {
	n := NewNode(
		ua.NewNumericNodeID(ns.ID(), id.RootFolder),
		map[ua.AttributeID]*ua.Variant{
			ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
			ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Root")),
			ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Root", "")),
		},
		[]*ua.ReferenceDescription{},
		nil,
	)
	return n

}
