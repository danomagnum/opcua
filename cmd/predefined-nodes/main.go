// Copyright 2018-2019 opcua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/server/refs"
	"github.com/gopcua/opcua/ua"
)

func main() {

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	fmt.Println(exPath)

	in := flag.String("in", "../../schema/Opc.Ua.PredefinedNodes.xml", "XML of predefined nodes")
	out := flag.String("out", "nodes_gen.go", "generated file")
	pkg := flag.String("pkg", "server", "package name")
	flag.Parse()

	log.SetFlags(0)

	f, err := os.Open(*in)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var nodes []*Node
	d := xml.NewDecoder(f)
	for {
		tok, err := d.Token()
		if tok == nil || err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		switch ty := tok.(type) {
		case xml.StartElement:
			if ty.Name.Space != "http://opcfoundation.org/UA/" {
				continue
			}
			n := new(Node)
			if err := d.DecodeElement(n, &ty); err != nil {
				log.Fatal(err)
			}
			n.Type = ty.Name.Local
			if n.NodeID.Identifier.IntID() == 86 {
				log.Printf("parsing types folder")
			}
			// fmt.Println(n.NodeID.Identifier.String())
			// fmt.Printf("%#v\n", n)
			nodes = append(nodes, n)
		}
	}

	m := map[string]*Node{}
	for _, n := range nodes {
		// trim the _xx suffix from the node class
		n.NodeClass = n.NodeClass[0:strings.Index(n.NodeClass, "_")] // DataType_32 -> DataType

		m[n.NodeID.Identifier.String()] = n
	}

	// create HasSubtype refs
	for _, n := range m {
		sid := n.SuperTypeID.Identifier.String()
		if sid == "" {
			continue
		}
		ref := refs.HasSubtype(&ua.ExpandedNodeID{NodeID: n.NodeID.Identifier})
		m[sid].Refs = append(m[sid].Refs, ref)
	}

	// create other refs
	for _, n := range m {

		for i := range n.References {
			ref := n.References[i]
			target_id := ref.TargetID
			o := m[target_id.Identifier.String()]
			if o == nil {
				log.Printf("found nil reference to id %v", target_id.Identifier.String())
				continue
			}
			if n.NodeID.Identifier.IntID() == 86 {
				log.Printf("Doing Types Folder")

			}

			eoid := ua.NewExpandedNodeID(target_id.Identifier, "", 0)

			newref := &ua.ReferenceDescription{
				ReferenceTypeID: ref.ReferenceTypeID.Identifier, //o.refs[0].ReferenceTypeID,
				IsForward:       !ref.IsInverse,
				NodeID:          eoid,
				BrowseName:      &ua.QualifiedName{0, o.BrowseName.Name},
				DisplayName:     &ua.LocalizedText{EncodingMask: ua.LocalizedTextText, Text: o.BrowseName.Name},
				TypeDefinition:  eoid,
			}
			n.Refs = append(n.Refs, newref)

			// if it's a reverse reference, we need to add it in the forward direction also maybe.

			eoid2 := ua.NewExpandedNodeID(n.NodeID.Identifier, "", 0)
			newref2 := &ua.ReferenceDescription{
				ReferenceTypeID: ref.ReferenceTypeID.Identifier, //o.refs[0].ReferenceTypeID,
				IsForward:       ref.IsInverse,
				NodeID:          eoid2,
				BrowseName:      &ua.QualifiedName{0, n.BrowseName.Name},
				DisplayName:     &ua.LocalizedText{EncodingMask: ua.LocalizedTextText, Text: n.BrowseName.Name},
				TypeDefinition:  eoid2,
			}
			o.Refs = append(o.Refs, newref2)

		}

	}

	data := map[string]interface{}{
		"Package": *pkg,
		"Nodes":   nodes,
	}

	var b bytes.Buffer
	if err := tmpl.Execute(&b, data); err != nil {
		log.Fatal(err)
	}
	src, err := format.Source(b.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	if *out != "" {
		if err := os.WriteFile(*out, src, 0644); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("Wrote %s/%s", *pkg, *out)
}

type Node struct {
	Type      string `xml:"-"`
	Xmlns     string `xml:",attr"`
	NodeClass string
	NodeID    struct {
		Identifier *ua.NodeID
	} `xml:"NodeId"`
	BrowseName struct {
		NamespaceIndex string
		Name           string
	}
	ReferenceTypeID struct {
		Identifier *ua.NodeID
	} `xml:"ReferenceTypeId"`
	TypeDefinitionID struct {
		Identifier *ua.NodeID
	} `xml:"TypeDefinitionId"`
	SuperTypeID struct {
		Identifier *ua.NodeID
	} `xml:"SuperTypeId"`
	InverseName *struct {
		Locale string
		Text   string
	}
	IsAbstract bool

	Refs       []*ua.ReferenceDescription
	References []Reference `xml:"References>Reference"`
}

type Reference struct {
	ReferenceTypeID struct {
		Identifier *ua.NodeID
	} `xml:"ReferenceTypeId"`
	IsInverse bool `xml:"IsInverse"`
	TargetID  struct {
		Identifier *ua.NodeID
	} `xml:"TargetId"`
}

var funcs = template.FuncMap{
	"idname": id.Name,
}

var tmpl = template.Must(template.New("").Funcs(funcs).Parse(`// Generated code. DO NOT EDIT
 // Copyright 2018-2023 opcua authors. All rights reserved.
 // Use of this source code is governed by a MIT-style license that can be
 // found in the LICENSE file.
 package {{.Package}}

 import (
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/server/attrs"
	"github.com/gopcua/opcua/ua"
 )

 func PredefinedNodes() []*Node{
 	return []*Node{
 {{- range .Nodes }}
 		NewNode(
 			{{- with .NodeID.Identifier }}
 			ua.NewNumericNodeID({{.Namespace}}, {{.IntID}}),
 			{{- end}}
 			map[ua.AttributeID]*ua.Variant{
 				ua.AttributeIDNodeClass: ua.MustVariant(int32(ua.NodeClass{{.NodeClass}})),
 				ua.AttributeIDBrowseName: ua.MustVariant(attrs.BrowseName("{{.BrowseName.Name}}")),
 				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("{{.BrowseName.Name}}", "")),
 				{{- with .InverseName }}
 				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("{{.Text}}", "{{.Locale}}")),
 				{{- end}}
 			},
			[]*ua.ReferenceDescription{
			{{- range .Refs }}
				{
					{{if .NodeID }}
					NodeID: ua.NewExpandedNodeID(ua.NewNumericNodeID(0, {{.NodeID.NodeID.IntID}}), "", {{.NodeID.NodeID.IntID}}),
					{{end}}
					{{if .BrowseName}}
					BrowseName:    &ua.QualifiedName{NamespaceIndex: 0, Name: "{{.BrowseName.Name}}"},
					{{end}}
					{{if .DisplayName}}
					DisplayName:   &ua.LocalizedText{EncodingMask: ua.LocalizedTextText, Text: "{{.DisplayName.Text}}"},
					{{end}}
					ReferenceTypeID: ua.NewNumericNodeID(0, id.{{idname .ReferenceTypeID.IntID}}),
					TypeDefinition: ua.NewNumericExpandedNodeID(0, {{.TypeDefinition.NodeID.IntID}}),
					IsForward: {{.IsForward}},
				},
 			{{- end }}
			},
			nil,
		),
 {{- end }}
 	}
 }
 `))
