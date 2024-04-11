package server

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/schema"
	"github.com/gopcua/opcua/server/attrs"
	"github.com/gopcua/opcua/ua"
)

// the base "node-centric" namespace
type NodeNameSpace struct {
	srv             *Server
	name            string
	mu              sync.RWMutex
	nodes           []*Node
	m               map[string]*Node
	id              uint16
	nodeid_sequence uint32

	ExternalNotification chan *ua.NodeID
}

func (ns *NodeNameSpace) GetNextNodeID() uint32 {
	if ns.nodeid_sequence < 100 {
		ns.nodeid_sequence = 100
	}
	return atomic.AddUint32(&(ns.nodeid_sequence), 1)
}

func NewNodeNameSpace(srv *Server, name string) *NodeNameSpace {
	ns := &NodeNameSpace{
		srv:                  srv,
		name:                 name,
		nodes:                make([]*Node, 0),
		m:                    make(map[string]*Node),
		ExternalNotification: make(chan *ua.NodeID),
	}
	srv.AddNamespace(ns)

	//objectsNode := NewFolderNode(ua.NewNumericNodeID(ns.id, id.ObjectsFolder), ns.name)
	oid := ua.NewNumericNodeID(ns.ID(), id.ObjectsFolder)
	//eoid := ua.NewNumericExpandedNodeID(ns.ID(), id.ObjectsFolder)
	typedef := ua.NewNumericExpandedNodeID(0, id.ObjectsFolder)
	//reftype := ua.NewTwoByteNodeID(uint8(id.HasComponent)) // folder
	objectsNode := NewNode(
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

	ns.AddNode(objectsNode)

	return ns

}

// This function is to notify opc subscribers if a node was changed
// without using the SetAttribute method
func (s *NodeNameSpace) ChangeNotification(nodeid *ua.NodeID) {
	s.srv.ChangeNotification(nodeid)
}

func (ns *NodeNameSpace) Name() string {
	return ns.name
}

func NewNameSpace(name string) *NodeNameSpace {
	return &NodeNameSpace{name: name, m: map[string]*Node{}}
}

func (as *NodeNameSpace) AddNode(n *Node) *Node {
	as.mu.Lock()
	defer as.mu.Unlock()

	/*
		nn := &Node{
			id:   n.id,
			attr: maps.Clone(n.attr),
			refs: slices.Clone(n.refs),
			val:  n.val,
			ns:   as,
		}
	*/

	// todo(fs): this is wrong since this leaves the old node in the list.
	as.nodes = append(as.nodes, n)
	k := n.ID().String()

	as.m[k] = n
	return n
}

func (as *NodeNameSpace) AddNewVariableNode(name string, value any) *Node {
	n := NewVariableNode(ua.NewNumericNodeID(as.id, as.GetNextNodeID()), name, value)
	as.AddNode(n)
	return n
}
func (as *NodeNameSpace) AddNewVariableStringNode(name string, value any) *Node {
	n := NewVariableNode(ua.NewStringNodeID(as.id, name), name, value)
	as.AddNode(n)
	return n
}

func (as *NodeNameSpace) Attribute(id *ua.NodeID, attr ua.AttributeID) *ua.DataValue {
	n := as.Node(id)
	if n == nil {
		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
			ServerTimestamp: time.Now(),
			Status:          ua.StatusBadNodeIDUnknown,
		}
	}
	var a *AttrValue
	var err error

	switch attr {
	case ua.AttributeIDNodeID:
		a = &AttrValue{Value: ua.MustVariant(id)}
	case ua.AttributeIDEventNotifier:
		// TODO: this is a hack to force the EventNotifier to false for everything.
		// If at some point someone or something needs to use this, this will have to go away and be
		// fixed properly.
		a = &AttrValue{Value: ua.MustVariant(byte(0))}
	case ua.AttributeIDNodeClass:
		a, err = n.Attribute(attr)
		if err != nil {
			return &ua.DataValue{
				EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
				ServerTimestamp: time.Now(),
				Status:          ua.StatusBadAttributeIDInvalid,
			}
		}
		// TODO: we need int32 instead of uint32 here.  this isn't the right place to fix it, but it is a bandaid
		x, ok := a.Value.Value().(uint32)
		if ok {
			a.Value = ua.MustVariant(int32(x))
		}
	default:
		a, err = n.Attribute(attr)
	}

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
		Value:           a.Value,
	}
}

func (as *NodeNameSpace) Node(id *ua.NodeID) *Node {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if id == nil {
		return nil
	}
	k := id.String()

	n := as.m[k]
	if n == nil {
		return nil
	}
	return n
}

func (as *NodeNameSpace) Objects() *Node {
	of := ua.NewNumericNodeID(as.id, id.ObjectsFolder)
	return as.Node(of)
}

func (as *NodeNameSpace) Root() *Node {
	return as.Node(RootFolder)
}

func (ns *NodeNameSpace) Browse(bd *ua.BrowseDescription) *ua.BrowseResult {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	if ns.srv.cfg.logger != nil {
		ns.srv.cfg.logger.Debug("BrowseRequest: id=%s mask=%08b\n", bd.NodeID, bd.ResultMask)
	}

	n := ns.Node(bd.NodeID)
	if n == nil {
		return &ua.BrowseResult{StatusCode: ua.StatusBadNodeIDUnknown}
	}

	refs := make([]*ua.ReferenceDescription, 0, len(n.refs))

	for i := range n.refs {
		r := n.refs[i]
		// we can't have nils in these or the encoder will fail.
		if r.NodeID == nil || r.BrowseName == nil || r.DisplayName == nil || r.TypeDefinition == nil {
			continue
		}

		// see if this is a ref the client was interested in.
		if !suitableRef(ns.srv, bd, r) {
			continue
		}

		td := ns.srv.Node(r.NodeID.NodeID)

		rf := &ua.ReferenceDescription{
			ReferenceTypeID: r.ReferenceTypeID,
			IsForward:       r.IsForward,
			NodeID:          r.NodeID,
			BrowseName:      r.BrowseName,
			DisplayName:     r.DisplayName,
			NodeClass:       r.NodeClass,
			TypeDefinition:  td.DataType(),
		}

		if rf.ReferenceTypeID.IntID() == id.HasTypeDefinition && rf.IsForward {
			// this one has to be first!
			refs = append([]*ua.ReferenceDescription{rf}, refs...)
		} else {
			refs = append(refs, rf)
		}
	}

	return &ua.BrowseResult{
		StatusCode: ua.StatusGood,
		References: refs,
	}

}

func (ns *NodeNameSpace) ID() uint16 {
	return ns.id
}

func (ns *NodeNameSpace) SetID(id uint16) {
	ns.id = id
}
func (as *NodeNameSpace) SetAttribute(id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
	n := as.Node(id)
	if n == nil {
		return ua.StatusBadNodeIDUnknown
	}

	access, err := n.Attribute(ua.AttributeIDUserAccessLevel)
	if err == nil {
		x := access.Value.Value()
		_ = x
		return ua.StatusBadUserAccessDenied
	}

	err = n.SetAttribute(attr, *val)
	if err != nil {
		return ua.StatusBadAttributeIDInvalid
	}
	as.srv.ChangeNotification(id)
	select {
	case as.ExternalNotification <- id:
	default:
	}

	return ua.StatusOK
}

func (srv *Server) ImportNodeSet(nodes *schema.UANodeSet) error {
	err := srv.namespacesImportNodeSet(nodes)
	if err != nil {
		return fmt.Errorf("problem creating namespaces: %w", err)
	}
	err = srv.nodesImportNodeSet(nodes)
	if err != nil {
		return fmt.Errorf("problem creating nodes: %w", err)
	}
	srv.refsImportNodeSet(nodes)
	if err != nil {
		return fmt.Errorf("problem creating references: %w", err)
	}
	return nil
}

func (srv *Server) namespacesImportNodeSet(nodes *schema.UANodeSet) error {
	if nodes.NamespaceUris == nil {
		return nil
	}
	for i := range nodes.NamespaceUris.Uri {
		_ = NewNodeNameSpace(srv, nodes.NamespaceUris.Uri[i])
	}
	return nil
}

func (srv *Server) nodesImportNodeSet(nodes *schema.UANodeSet) error {

	log.Printf("New Node Set: %s", nodes.LastModifiedAttr)

	reftypes := make(map[string]*schema.UAReferenceType)

	// the first thing we have to do is go thorugh and define all the nodes.
	// set up the reference types.
	for i := range nodes.UAReferenceType {
		rt := nodes.UAReferenceType[i]
		reftypes[rt.BrowseNameAttr] = rt // sometimes they use browse name
		reftypes[rt.NodeIdAttr] = rt     // sometimes they use node id

		nid := ua.MustParseNodeID(rt.NodeIdAttr)

		var attrs Attributes = make(map[ua.AttributeID]*ua.Variant)
		attrs[ua.AttributeIDAccessRestrictions] = ua.MustVariant(rt.AccessRestrictionsAttr)
		attrs[ua.AttributeIDBrowseName] = ua.MustVariant(&ua.QualifiedName{NamespaceIndex: nid.Namespace(), Name: rt.BrowseNameAttr})
		attrs[ua.AttributeIDIsAbstract] = ua.MustVariant(rt.IsAbstractAttr)
		attrs[ua.AttributeIDUserWriteMask] = ua.MustVariant(rt.UserWriteMaskAttr)
		attrs[ua.AttributeIDSymmetric] = ua.MustVariant(rt.SymmetricAttr)
		attrs[ua.AttributeIDWriteMask] = ua.MustVariant(rt.WriteMaskAttr)
		if len(rt.DisplayName) > 0 {
			attrs[ua.AttributeIDDisplayName] = ua.MustVariant(ua.NewLocalizedText(rt.DisplayName[0].Value))
		}
		if len(rt.InverseName) > 0 {
			attrs[ua.AttributeIDInverseName] = ua.MustVariant(ua.NewLocalizedText(rt.InverseName[0].Value))
		} else {
			attrs[ua.AttributeIDInverseName] = ua.MustVariant(ua.NewLocalizedText(""))
		}
		if len(rt.Description) > 0 {
			attrs[ua.AttributeIDDescription] = ua.MustVariant(ua.NewLocalizedText(rt.Description[0].Value))
		}
		attrs[ua.AttributeIDNodeClass] = ua.MustVariant(uint32(ua.NodeClassReferenceType))

		var refs References = make([]*ua.ReferenceDescription, 0)

		n := NewNode(nid, attrs, refs, nil)
		ns, err := srv.Namespace(int(nid.Namespace()))
		if err != nil {
			// This namespace doesn't exist.
			if srv.cfg.logger != nil {
				srv.cfg.logger.Warn("Could Not Find Namespace %d", nid.Namespace())
			}
			return err
		}
		ns.AddNode(n)
	}

	// set up the data types.
	for i := range nodes.UADataType {
		dt := nodes.UADataType[i]
		nid := ua.MustParseNodeID(dt.NodeIdAttr)

		var attrs Attributes = make(map[ua.AttributeID]*ua.Variant)
		attrs[ua.AttributeIDAccessRestrictions] = ua.MustVariant(dt.AccessRestrictionsAttr)
		attrs[ua.AttributeIDBrowseName] = ua.MustVariant(&ua.QualifiedName{NamespaceIndex: nid.Namespace(), Name: dt.BrowseNameAttr})
		attrs[ua.AttributeIDIsAbstract] = ua.MustVariant(dt.IsAbstractAttr)
		attrs[ua.AttributeIDUserWriteMask] = ua.MustVariant(dt.UserWriteMaskAttr)
		attrs[ua.AttributeIDWriteMask] = ua.MustVariant(dt.WriteMaskAttr)
		if len(dt.DisplayName) > 0 {
			attrs[ua.AttributeIDDisplayName] = ua.MustVariant(ua.NewLocalizedText(dt.DisplayName[0].Value))
		}
		if len(dt.Description) > 0 {
			attrs[ua.AttributeIDDescription] = ua.MustVariant(ua.NewLocalizedText(dt.Description[0].Value))
		}
		attrs[ua.AttributeIDNodeClass] = ua.MustVariant(uint32(ua.NodeClassDataType))

		var refs References = make([]*ua.ReferenceDescription, 0)

		n := NewNode(nid, attrs, refs, nil)

		ns, err := srv.Namespace(int(nid.Namespace()))
		if err != nil {
			// This namespace doesn't exist.
			if srv.cfg.logger != nil {
				srv.cfg.logger.Warn("Could Not Find Namespace %d", nid.Namespace())
			}
			return err
		}
		ns.AddNode(n)
	}

	// set up the object types
	for i := range nodes.UAObjectType {
		ot := nodes.UAObjectType[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		var attrs Attributes = make(map[ua.AttributeID]*ua.Variant)
		attrs[ua.AttributeIDAccessRestrictions] = ua.MustVariant(ot.AccessRestrictionsAttr)
		attrs[ua.AttributeIDBrowseName] = ua.MustVariant(&ua.QualifiedName{NamespaceIndex: nid.Namespace(), Name: ot.BrowseNameAttr})
		attrs[ua.AttributeIDIsAbstract] = ua.MustVariant(ot.IsAbstractAttr)
		attrs[ua.AttributeIDUserWriteMask] = ua.MustVariant(ot.UserWriteMaskAttr)
		attrs[ua.AttributeIDWriteMask] = ua.MustVariant(ot.WriteMaskAttr)
		if len(ot.DisplayName) > 0 {
			attrs[ua.AttributeIDDisplayName] = ua.MustVariant(ua.NewLocalizedText(ot.DisplayName[0].Value))
		}
		if len(ot.Description) > 0 {
			attrs[ua.AttributeIDDescription] = ua.MustVariant(ua.NewLocalizedText(ot.Description[0].Value))
		}
		attrs[ua.AttributeIDNodeClass] = ua.MustVariant(uint32(ua.NodeClassObjectType))

		var refs References = make([]*ua.ReferenceDescription, 0)

		n := NewNode(nid, attrs, refs, nil)
		ns, err := srv.Namespace(int(nid.Namespace()))
		if err != nil {
			// This namespace doesn't exist.
			if srv.cfg.logger != nil {
				srv.cfg.logger.Warn("Could Not Find Namespace %d", nid.Namespace())
			}
			return err
		}
		ns.AddNode(n)
	}

	// set up the variable Types
	for i := range nodes.UAVariableType {
		ot := nodes.UAVariableType[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		var attrs Attributes = make(map[ua.AttributeID]*ua.Variant)
		attrs[ua.AttributeIDAccessRestrictions] = ua.MustVariant(ot.AccessRestrictionsAttr)
		attrs[ua.AttributeIDBrowseName] = ua.MustVariant(&ua.QualifiedName{NamespaceIndex: nid.Namespace(), Name: ot.BrowseNameAttr})
		attrs[ua.AttributeIDUserWriteMask] = ua.MustVariant(ot.UserWriteMaskAttr)
		attrs[ua.AttributeIDWriteMask] = ua.MustVariant(ot.WriteMaskAttr)
		if len(ot.DisplayName) > 0 {
			attrs[ua.AttributeIDDisplayName] = ua.MustVariant(ua.NewLocalizedText(ot.DisplayName[0].Value))
		}
		if len(ot.Description) > 0 {
			attrs[ua.AttributeIDDescription] = ua.MustVariant(ua.NewLocalizedText(ot.Description[0].Value))
		}
		attrs[ua.AttributeIDNodeClass] = ua.MustVariant(uint32(ua.NodeClassVariableType))

		var refs References = make([]*ua.ReferenceDescription, 0)

		n := NewNode(nid, attrs, refs, nil)
		ns, err := srv.Namespace(int(nid.Namespace()))
		if err != nil {
			// This namespace doesn't exist.
			if srv.cfg.logger != nil {
				srv.cfg.logger.Warn("Could Not Find Namespace %d", nid.Namespace())
			}
			return err
		}
		ns.AddNode(n)
	}

	// set up the variables
	for i := range nodes.UAVariable {
		ot := nodes.UAVariable[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		var attrs Attributes = make(map[ua.AttributeID]*ua.Variant)
		attrs[ua.AttributeIDAccessRestrictions] = ua.MustVariant(ot.AccessRestrictionsAttr)
		attrs[ua.AttributeIDBrowseName] = ua.MustVariant(&ua.QualifiedName{NamespaceIndex: nid.Namespace(), Name: ot.BrowseNameAttr})
		attrs[ua.AttributeIDUserWriteMask] = ua.MustVariant(ot.UserWriteMaskAttr)
		attrs[ua.AttributeIDWriteMask] = ua.MustVariant(ot.WriteMaskAttr)
		if len(ot.DisplayName) > 0 {
			attrs[ua.AttributeIDDisplayName] = ua.MustVariant(ua.NewLocalizedText(ot.DisplayName[0].Value))
		}
		if len(ot.Description) > 0 {
			attrs[ua.AttributeIDDescription] = ua.MustVariant(ua.NewLocalizedText(ot.Description[0].Value))
		}
		attrs[ua.AttributeIDNodeClass] = ua.MustVariant(uint32(ua.NodeClassVariable))

		var refs References = make([]*ua.ReferenceDescription, 0)

		n := NewNode(nid, attrs, refs, nil)
		ns, err := srv.Namespace(int(nid.Namespace()))
		if err != nil {
			// This namespace doesn't exist.
			if srv.cfg.logger != nil {
				srv.cfg.logger.Warn("Could Not Find Namespace %d", nid.Namespace())
			}
			return err
		}
		ns.AddNode(n)
	}

	// set up the methods
	for i := range nodes.UAMethod {
		ot := nodes.UAMethod[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		var attrs Attributes = make(map[ua.AttributeID]*ua.Variant)
		attrs[ua.AttributeIDAccessRestrictions] = ua.MustVariant(ot.AccessRestrictionsAttr)
		attrs[ua.AttributeIDBrowseName] = ua.MustVariant(&ua.QualifiedName{NamespaceIndex: nid.Namespace(), Name: ot.BrowseNameAttr})
		attrs[ua.AttributeIDUserWriteMask] = ua.MustVariant(ot.UserWriteMaskAttr)
		attrs[ua.AttributeIDWriteMask] = ua.MustVariant(ot.WriteMaskAttr)
		if len(ot.DisplayName) > 0 {
			attrs[ua.AttributeIDDisplayName] = ua.MustVariant(ua.NewLocalizedText(ot.DisplayName[0].Value))
		}
		if len(ot.Description) > 0 {
			attrs[ua.AttributeIDDescription] = ua.MustVariant(ua.NewLocalizedText(ot.Description[0].Value))
		}
		attrs[ua.AttributeIDNodeClass] = ua.MustVariant(uint32(ua.NodeClassMethod))

		var refs References = make([]*ua.ReferenceDescription, 0)

		n := NewNode(nid, attrs, refs, nil)
		ns, err := srv.Namespace(int(nid.Namespace()))
		if err != nil {
			// This namespace doesn't exist.
			if srv.cfg.logger != nil {
				srv.cfg.logger.Warn("Could Not Find Namespace %d", nid.Namespace())
			}
			return err
		}
		ns.AddNode(n)
	}

	// set up the objects
	for i := range nodes.UAObject {
		ot := nodes.UAObject[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		if ot.NodeIdAttr == "i=85" {
			log.Printf("doing objects.")
		}
		var attrs Attributes = make(map[ua.AttributeID]*ua.Variant)
		attrs[ua.AttributeIDAccessRestrictions] = ua.MustVariant(ot.AccessRestrictionsAttr)
		attrs[ua.AttributeIDBrowseName] = ua.MustVariant(&ua.QualifiedName{NamespaceIndex: nid.Namespace(), Name: ot.BrowseNameAttr})
		attrs[ua.AttributeIDUserWriteMask] = ua.MustVariant(ot.UserWriteMaskAttr)
		attrs[ua.AttributeIDWriteMask] = ua.MustVariant(ot.WriteMaskAttr)
		if len(ot.DisplayName) > 0 {
			attrs[ua.AttributeIDDisplayName] = ua.MustVariant(ua.NewLocalizedText(ot.DisplayName[0].Value))
		}
		if len(ot.Description) > 0 {
			attrs[ua.AttributeIDDescription] = ua.MustVariant(ua.NewLocalizedText(ot.Description[0].Value))
		}

		attrs[ua.AttributeIDNodeClass] = ua.MustVariant(uint32(ua.NodeClassObject))

		var refs References = make([]*ua.ReferenceDescription, 0)

		n := NewNode(nid, attrs, refs, nil)
		ns, err := srv.Namespace(int(nid.Namespace()))
		if err != nil {
			// This namespace doesn't exist.
			if srv.cfg.logger != nil {
				srv.cfg.logger.Warn("Could Not Find Namespace %d", nid.Namespace())
			}
			return err
		}
		ns.AddNode(n)
	}

	return nil
}
func (srv *Server) refsImportNodeSet(nodes *schema.UANodeSet) error {

	log.Printf("New Node Set: %s", nodes.LastModifiedAttr)

	failures := 0
	reftypes := make(map[string]*schema.UAReferenceType)
	for i := range nodes.UAReferenceType {
		rt := nodes.UAReferenceType[i]
		reftypes[rt.BrowseNameAttr] = rt // sometimes they use browse name
		reftypes[rt.NodeIdAttr] = rt     // sometimes they use node id
	}

	aliases := make(map[string]string)
	for i := range nodes.Aliases.Alias {
		alias := nodes.Aliases.Alias[i]
		aliases[alias.AliasAttr] = alias.Value
	}

	// any of the aliases could be reference types, so we have to check them all and add them to the reftypes map
	// if they are.
	for alias := range aliases {
		aliasID := ua.MustParseNodeID(aliases[alias])
		refnode := srv.Node(aliasID)
		if refnode == nil {
			if srv.cfg.logger != nil {
				srv.cfg.logger.Warn("error loading alias %s", alias)
			}
			continue
		}
		rt := new(schema.UAReferenceType)
		rt.UAType = new(schema.UAType)
		rt.UAType.UANode = new(schema.UANode)
		rt.BrowseNameAttr = alias
		rt.NodeIdAttr = aliases[alias]
		isSymmetricValue, err := refnode.Attribute(ua.AttributeIDSymmetric)
		if err == nil {
			rt.SymmetricAttr = isSymmetricValue.Value.Value().(bool)
		}

		_, ok := reftypes[alias]
		if !ok {
			reftypes[alias] = rt // sometimes they use browse name
		} else {
			if srv.cfg.logger != nil {
				srv.cfg.logger.Error("Duplicate reference type %s", alias)
			}
			continue
		}

		_, ok = reftypes[aliases[alias]]
		if !ok {
			reftypes[aliases[alias]] = rt // sometimes they use node id
		} else {
			if srv.cfg.logger != nil {
				srv.cfg.logger.Error("Duplicate reference type %s", aliases[alias])
			}
			continue
		}

	}

	// the first thing we have to do is go thorugh and define all the nodes.
	// set up the reference types.
	for i := range nodes.UAReferenceType {
		rt := nodes.UAReferenceType[i]

		nodeid := ua.MustParseNodeID(rt.NodeIdAttr)
		node := srv.Node(nodeid)
		if node == nil {
			log.Printf("Error loading node %s", rt.NodeIdAttr)
		}

		for rid := range rt.References.Reference {
			ref := rt.References.Reference[rid]
			refnodeid := ua.MustParseNodeID(ref.Value)
			n := srv.Node(refnodeid)
			if n == nil {
				log.Printf("can't find node %s as %s reference to %s", ref.Value, ref.ReferenceTypeAttr, rt.BrowseNameAttr)
				failures++
				continue
			}

			if ref.IsForwardAttr == nil {
				v := true
				ref.IsForwardAttr = &v
			}
			reftypeid := ua.MustParseNodeID(reftypes[ref.ReferenceTypeAttr].NodeIdAttr)
			node.AddRef(n, RefType(reftypeid.IntID()), *ref.IsForwardAttr)
			if !reftypes[ref.ReferenceTypeAttr].SymmetricAttr {
				n.AddRef(node, RefType(reftypeid.IntID()), !*ref.IsForwardAttr)
			}
		}

	}

	// set up the data types.
	for i := range nodes.UADataType {
		dt := nodes.UADataType[i]
		nid := ua.MustParseNodeID(dt.NodeIdAttr)
		node := srv.Node(nid)

		if nid.IntID() == 24 {
			log.Printf("doing BaseDataType")
		}

		for rid := range dt.References.Reference {
			ref := dt.References.Reference[rid]
			refnodeid := ua.MustParseNodeID(ref.Value)
			n := srv.Node(refnodeid)
			if n == nil {
				log.Printf("can't find node %s as %s reference to %s", ref.Value, ref.ReferenceTypeAttr, dt.BrowseNameAttr)
				failures++
				continue
			}

			if ref.IsForwardAttr == nil {
				v := true
				ref.IsForwardAttr = &v
			}

			reftypeid := ua.MustParseNodeID(reftypes[ref.ReferenceTypeAttr].NodeIdAttr)
			node.AddRef(n, RefType(reftypeid.IntID()), *ref.IsForwardAttr)
			if !reftypes[ref.ReferenceTypeAttr].SymmetricAttr {
				n.AddRef(node, RefType(reftypeid.IntID()), !*ref.IsForwardAttr)
			}

		}

	}

	// set up the object types
	for i := range nodes.UAObjectType {
		ot := nodes.UAObjectType[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		node := srv.Node(nid)

		for rid := range ot.References.Reference {
			ref := ot.References.Reference[rid]
			refnodeid := ua.MustParseNodeID(ref.Value)
			n := srv.Node(refnodeid)
			if n == nil {
				log.Printf("can't find node %s as %s reference to %s", ref.Value, ref.ReferenceTypeAttr, ot.BrowseNameAttr)
				failures++
				continue
			}
			if ref.IsForwardAttr == nil {
				v := true
				ref.IsForwardAttr = &v
			}
			reftypeid := ua.MustParseNodeID(reftypes[ref.ReferenceTypeAttr].NodeIdAttr)
			node.AddRef(n, RefType(reftypeid.IntID()), *ref.IsForwardAttr)
			if !reftypes[ref.ReferenceTypeAttr].SymmetricAttr {
				n.AddRef(node, RefType(reftypeid.IntID()), !*ref.IsForwardAttr)
			}
		}
	}

	// set up the variable Types
	for i := range nodes.UAVariableType {
		ot := nodes.UAVariableType[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		node := srv.Node(nid)

		for rid := range ot.References.Reference {
			ref := ot.References.Reference[rid]
			refnodeid := ua.MustParseNodeID(ref.Value)
			n := srv.Node(refnodeid)
			if n == nil {
				log.Printf("can't find node %s as %s reference to %s", ref.Value, ref.ReferenceTypeAttr, ot.BrowseNameAttr)
				failures++
				continue
			}
			if ref.IsForwardAttr == nil {
				v := true
				ref.IsForwardAttr = &v
			}
			reftypeid := ua.MustParseNodeID(reftypes[ref.ReferenceTypeAttr].NodeIdAttr)
			node.AddRef(n, RefType(reftypeid.IntID()), *ref.IsForwardAttr)
			if !reftypes[ref.ReferenceTypeAttr].SymmetricAttr {
				n.AddRef(node, RefType(reftypeid.IntID()), !*ref.IsForwardAttr)
			}

		}

	}

	// set up the variables
	for i := range nodes.UAVariable {
		ot := nodes.UAVariable[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		node := srv.Node(nid)

		for rid := range ot.References.Reference {
			ref := ot.References.Reference[rid]
			refnodeid := ua.MustParseNodeID(ref.Value)
			n := srv.Node(refnodeid)
			if n == nil {
				log.Printf("can't find node %s as %s reference to %s", ref.Value, ref.ReferenceTypeAttr, ot.BrowseNameAttr)
				failures++
				continue
			}
			if ref.IsForwardAttr == nil {
				v := true
				ref.IsForwardAttr = &v
			}
			reftypeid := ua.MustParseNodeID(reftypes[ref.ReferenceTypeAttr].NodeIdAttr)
			node.AddRef(n, RefType(reftypeid.IntID()), *ref.IsForwardAttr)
			if !reftypes[ref.ReferenceTypeAttr].SymmetricAttr {
				n.AddRef(node, RefType(reftypeid.IntID()), !*ref.IsForwardAttr)
			}

		}

	}

	// set up the methods
	for i := range nodes.UAMethod {
		ot := nodes.UAMethod[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		node := srv.Node(nid)

		for rid := range ot.References.Reference {
			ref := ot.References.Reference[rid]
			refnodeid := ua.MustParseNodeID(ref.Value)
			n := srv.Node(refnodeid)
			if n == nil {
				log.Printf("can't find node %s as %s reference to %s", ref.Value, ref.ReferenceTypeAttr, ot.BrowseNameAttr)
				failures++
				continue
			}
			if ref.IsForwardAttr == nil {
				v := true
				ref.IsForwardAttr = &v
			}
			reftypeid := ua.MustParseNodeID(reftypes[ref.ReferenceTypeAttr].NodeIdAttr)
			node.AddRef(n, RefType(reftypeid.IntID()), *ref.IsForwardAttr)
			if !reftypes[ref.ReferenceTypeAttr].SymmetricAttr {
				n.AddRef(node, RefType(reftypeid.IntID()), !*ref.IsForwardAttr)
			}
		}

	}

	// set up the objects
	for i := range nodes.UAObject {
		ot := nodes.UAObject[i]
		nid := ua.MustParseNodeID(ot.NodeIdAttr)
		node := srv.Node(nid)
		if ot.NodeIdAttr == "i=84" {
			log.Printf("doing root.")
		}

		for rid := range ot.References.Reference {
			ref := ot.References.Reference[rid]
			refnodeid := ua.MustParseNodeID(ref.Value)
			n := srv.Node(refnodeid)
			if n == nil {
				log.Printf("can't find node %s as %s reference to %s", ref.Value, ref.ReferenceTypeAttr, ot.BrowseNameAttr)
				failures++
				continue
			}
			if ref.IsForwardAttr == nil {
				v := true
				ref.IsForwardAttr = &v
			}
			reftypeid := ua.MustParseNodeID(reftypes[ref.ReferenceTypeAttr].NodeIdAttr)
			node.AddRef(n, RefType(reftypeid.IntID()), *ref.IsForwardAttr)
			if !reftypes[ref.ReferenceTypeAttr].SymmetricAttr {
				n.AddRef(node, RefType(reftypeid.IntID()), !*ref.IsForwardAttr)
			}

		}

	}

	return nil
}
