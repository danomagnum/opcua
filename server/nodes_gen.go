// Generated code. DO NOT EDIT
// Copyright 2018-2023 opcua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.
package server

import (
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/server/attrs"
	"github.com/gopcua/opcua/ua"
)

func PredefinedNodes() []*Node {
	return []*Node{
		NewNode(
			ua.NewNumericNodeID(0, 3062),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3063),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 20),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 16),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 13),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 26),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 29),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 22),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 1),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 26),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Number")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Number", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 27),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 50),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 28),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 10),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 27),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Integer")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Integer", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 8),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 4),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 6),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 28),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UInteger")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UInteger", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 5),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 7),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 29),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Enumeration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Enumeration", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11939),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 302),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 576),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24212),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 303),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15874),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 307),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24220),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 315),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24214),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 257),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 851),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15632),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 120),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 256),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24210),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24222),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24224),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 890),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 348),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24218),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24216),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11234),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19723),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19730),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 20408),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 98),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12552),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14647),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15008),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11293),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 852),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12077),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 1),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Boolean")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Boolean", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SByte")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SByte", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Byte")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Byte", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15031),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15033),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 4),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Int16")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Int16", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 5),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UInt16")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UInt16", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32251),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15904),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 95),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 6),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Int32")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Int32", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 7),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UInt32")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UInt32", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15642),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17588),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 288),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 347),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23564),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 289),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15654),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25517),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 20998),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 94),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15583),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24277),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24279),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15658),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15646),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15406),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 31917),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Int64")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Int64", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UInt64")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UInt64", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11737),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 10),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Float")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Float", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Double")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Double", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 290),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("String")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("String", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12881),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25726),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23751),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 295),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12879),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24263),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12877),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 31918),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 291),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12878),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12880),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 13),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DateTime")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DateTime", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 294),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Guid")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Guid", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ByteString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ByteString", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 521),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 311),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 16307),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 30),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("XmlElement")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("XmlElement", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NodeId")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NodeId", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 388),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExpandedNodeId")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExpandedNodeId", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StatusCode")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StatusCode", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 20),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("QualifiedName")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("QualifiedName", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("LocalizedText")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("LocalizedText", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 22),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Structure")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Structure", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 859),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 376),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15534),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12890),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12079),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 97),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15621),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 537),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15530),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 385),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18809),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 887),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15578),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 659),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 877),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 871),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23498),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14525),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15597),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11943),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15617),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25270),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15634),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12171),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 8912),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 862),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14593),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 316),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23601),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18811),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14533),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17548),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24281),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 894),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25520),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 583),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15605),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 382),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 865),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 432),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 304),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 891),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25220),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 948),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12755),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 312),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 296),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32659),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12189),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15622),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 7594),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 344),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15528),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15630),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 868),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15618),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 16313),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24107),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14524),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18806),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 856),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 308),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15502),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 96),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 586),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 299),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15609),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24105),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 589),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 920),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12080),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 540),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15616),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14744),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12756),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23468),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24033),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15623),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 379),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 338),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 874),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32660),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 331),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 101),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15598),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12554),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25519),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 897),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15611),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 884),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14273),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 719),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11944),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23603),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15580),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 853),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18807),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18813),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15629),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15628),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12172),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32285),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24106),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataValue")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataValue", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DiagnosticInfo")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DiagnosticInfo", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 30),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Image")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Image", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2000),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2002),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2003),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2001),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 50),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Decimal")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Decimal", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 31),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("References")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("References", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 33),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NonHierarchicalReferences")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NonHierarchicalReferences", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17603),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 37),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23469),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 52),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 41),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23562),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25253),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9005),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24137),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 54),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9006),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 53),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25237),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 51),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25257),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 38),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25255),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9004),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25258),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 39),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 40),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17597),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 117),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 33),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HierarchicalReferences")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HierarchicalReferences", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("InverseHierarchicalReferences", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 36),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 35),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25256),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14936),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25238),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 34),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25254),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25345),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 34),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasChild")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasChild", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("ChildOf", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 45),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 44),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32679),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 35),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Organizes")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Organizes", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("OrganizedBy", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 16362),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 36),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasEventSource")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasEventSource", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("EventSourceOf", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 48),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 37),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasModellingRule")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasModellingRule", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("ModellingRuleOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 38),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasEncoding")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasEncoding", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("EncodingOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 39),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasDescription", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("DescriptionOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 40),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasTypeDefinition")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasTypeDefinition", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("TypeDefinitionOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 41),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("GeneratesEvent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("GeneratesEvent", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("GeneratedBy", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3065),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3065),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlwaysGeneratesEvent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlwaysGeneratesEvent", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("AlwaysGeneratedBy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 44),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Aggregates")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Aggregates", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("AggregatedBy", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 56),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 47),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 46),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 45),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasSubtype")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasSubtype", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("SubtypeOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 46),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasProperty")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasProperty", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("PropertyOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 47),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasComponent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasComponent", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("ComponentOf", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 49),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24136),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15297),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25262),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 129),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15296),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18804),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15112),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 16361),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17604),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18805),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14476),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 48),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasNotifier")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasNotifier", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("NotifierOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 49),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasOrderedComponent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasOrderedComponent", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("OrderedComponentOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 51),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FromState")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FromState", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("ToTransition", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 52),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ToState")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ToState", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("FromTransition", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 53),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasCause")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasCause", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("MayBeCausedBy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 54),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasEffect")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasEffect", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("MayBeEffectedBy", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17276),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17984),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17983),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17985),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 117),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasSubStateMachine")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasSubStateMachine", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("SubStateMachineOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 56),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasHistoricalConfiguration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasHistoricalConfiguration", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("HistoricalConfigurationOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24136),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasStructuredComponent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasStructuredComponent", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsStructuredComponentOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24137),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AssociatedWith")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AssociatedWith", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 58),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseObjectType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseObjectType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11645),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21145),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23455),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2033),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2020),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21091),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11163),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17602),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2026),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12555),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19677),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 77),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32286),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25337),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15108),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17589),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17997),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2034),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25221),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14209),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17279),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23518),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15489),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12581),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14509),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11575),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2004),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2340),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 61),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2041),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11187),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15306),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2310),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15305),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2029),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18001),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2307),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 75),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12556),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14643),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25227),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23828),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14232),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15298),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15744),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 26871),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15471),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21104),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21096),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21090),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23832),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2330),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24264),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15620),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15607),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17852),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15319),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11616),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15906),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2013),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17998),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2299),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2318),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17721),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 76),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 61),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FolderType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14477),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15452),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23556),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25346),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 16405),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11564),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23795),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23456),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17591),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 13353),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17496),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 13813),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 62),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseVariableType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 68),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 63),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 63),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseDataVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseDataVariableType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2138),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2137),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17277),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2762),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17709),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19725),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17986),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2243),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2380),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18772),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2365),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 72),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2165),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 16309),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3051),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2164),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9002),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15383),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2755),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 69),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2171),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2150),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2244),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18779),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32244),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32657),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2196),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15113),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18786),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17714),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2172),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11487),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2197),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 68),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PropertyType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PropertyType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 69),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataTypeDescriptionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataTypeDescriptionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 72),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataTypeDictionaryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataTypeDictionaryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 75),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataTypeSystemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataTypeSystemType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 76),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataTypeEncodingType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataTypeEncodingType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 120),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NamingRuleType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NamingRuleType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 77),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ModellingRuleType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ModellingRuleType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 78),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Mandatory")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Mandatory", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 80),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Optional")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Optional", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 83),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExposesItsArray")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExposesItsArray", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11508),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OptionalPlaceholder")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OptionalPlaceholder", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11510),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MandatoryPlaceholder")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MandatoryPlaceholder", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 84),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Root")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Root", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.Organizes),
					NodeID:          &ua.ExpandedNodeID{NodeID: ua.NewNumericNodeID(0, 85)},
					BrowseName:      attrs.BrowseName("Objects"),
					DisplayName:     attrs.DisplayName("Objects", ""),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, id.DataTypesFolder),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 85),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Objects")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Objects", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 86),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Types")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Types", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 87),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Views")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Views", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 88),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ObjectTypes")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ObjectTypes", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 89),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("VariableTypes")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("VariableTypes", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 90),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataTypes")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataTypes", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 91),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReferenceTypes")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReferenceTypes", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 92),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("XML Schema")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("XML Schema", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 93),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OPC Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OPC Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 129),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasArgumentDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasArgumentDescription", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("ArgumentDescriptionOf", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 131),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 131),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasOptionalInputArgumentDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasOptionalInputArgumentDescription", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("OptionalInputArgumentDescriptionOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15957),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("http://opcfoundation.org/UA/")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("http://opcfoundation.org/UA/", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3068),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NodeVersion")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NodeVersion", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12170),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ViewVersion")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ViewVersion", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3067),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Icon")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Icon", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3069),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("LocalTime")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("LocalTime", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3070),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AllowNulls")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AllowNulls", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11433),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ValueAsText")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ValueAsText", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11498),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MaxStringLength")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MaxStringLength", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15002),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MaxCharacters")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MaxCharacters", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12908),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MaxByteStringLength")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MaxByteStringLength", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11512),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MaxArrayLength")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MaxArrayLength", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11513),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EngineeringUnits")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EngineeringUnits", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11432),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EnumStrings")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EnumStrings", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3071),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EnumValues")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EnumValues", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12745),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OptionSetValues")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OptionSetValues", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32750),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OptionSetLength")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OptionSetLength", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3072),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("InputArguments")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("InputArguments", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3073),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OutputArguments")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OutputArguments", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17605),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DefaultInstanceBrowseName")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DefaultInstanceBrowseName", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2000),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ImageBMP")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ImageBMP", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2001),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ImageGIF")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ImageGIF", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2002),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ImageJPG")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ImageJPG", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2003),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ImagePNG")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ImagePNG", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16307),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AudioDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AudioDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12756),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Union")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Union", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23751),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UriString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UriString", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2004),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2013),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerCapabilitiesType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerCapabilitiesType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2020),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerDiagnosticsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerDiagnosticsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2026),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionsDiagnosticsSummaryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionsDiagnosticsSummaryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2029),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionDiagnosticsObjectType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionDiagnosticsObjectType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2033),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("VendorServerInfoType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("VendorServerInfoType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2034),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerRedundancyType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerRedundancyType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2039),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2036),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2036),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TransparentRedundancyType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TransparentRedundancyType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2039),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NonTransparentRedundancyType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NonTransparentRedundancyType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11945),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11945),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NonTransparentNetworkRedundancyType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NonTransparentNetworkRedundancyType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11564),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OperationLimitsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OperationLimitsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11575),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FileType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FileType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12522),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11595),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25482),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11595),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AddressSpaceFileType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AddressSpaceFileType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11616),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NamespaceMetadataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NamespaceMetadataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11645),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NamespacesType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NamespacesType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2041),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2130),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2782),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2052),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2132),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2738),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11436),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2311),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3035),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2052),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12561),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2127),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23606),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12620),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2090),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2099),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2058),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2058),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditSecurityEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditSecurityEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2069),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2080),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2059),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2059),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditChannelEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditChannelEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2060),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2060),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditOpenSecureChannelEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditOpenSecureChannelEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2069),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditSessionEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditSessionEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2075),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2078),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2071),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2071),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCreateSessionEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCreateSessionEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2748),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2748),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditUrlMismatchEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditUrlMismatchEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2075),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditActivateSessionEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditActivateSessionEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2078),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCancelEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCancelEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2080),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCertificateEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCertificateEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2088),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2087),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2089),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2085),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2086),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2082),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2082),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCertificateDataMismatchEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCertificateDataMismatchEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2085),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCertificateExpiredEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCertificateExpiredEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2086),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCertificateInvalidEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCertificateInvalidEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2087),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCertificateUntrustedEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCertificateUntrustedEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2088),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCertificateRevokedEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCertificateRevokedEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2089),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditCertificateMismatchEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditCertificateMismatchEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2090),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditNodeManagementEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditNodeManagementEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2091),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2093),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2095),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2097),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2091),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditAddNodesEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditAddNodesEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2093),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditDeleteNodesEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditDeleteNodesEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2095),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditAddReferencesEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditAddReferencesEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2097),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditDeleteReferencesEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditDeleteReferencesEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2099),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditUpdateEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditUpdateEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2104),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2100),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2100),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditWriteUpdateEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditWriteUpdateEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2104),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditHistoryUpdateEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditHistoryUpdateEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19095),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2999),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3006),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3012),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2127),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditUpdateMethodEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditUpdateMethodEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17641),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2790),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32260),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2315),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32306),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18011),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2130),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SystemEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SystemEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2787),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2131),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2788),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2789),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15535),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11446),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2131),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DeviceFailureEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DeviceFailureEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11446),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SystemStatusChangeEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SystemStatusChangeEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2132),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseModelChangeEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseModelChangeEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2133),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2133),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("GeneralModelChangeEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("GeneralModelChangeEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2738),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SemanticChangeEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SemanticChangeEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3035),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EventQueueOverflowEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EventQueueOverflowEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11436),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProgressEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProgressEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23606),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditClientEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditClientEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23926),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23926),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditClientUpdateMethodResultEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditClientUpdateMethodResultEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2340),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AggregateFunctionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AggregateFunctionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2137),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerVendorCapabilityType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerVendorCapabilityType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2138),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerStatusType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerStatusType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3051),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BuildInfoType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BuildInfoType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2150),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerDiagnosticsSummaryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerDiagnosticsSummaryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2164),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SamplingIntervalDiagnosticsArrayType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SamplingIntervalDiagnosticsArrayType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2165),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SamplingIntervalDiagnosticsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SamplingIntervalDiagnosticsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2171),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SubscriptionDiagnosticsArrayType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SubscriptionDiagnosticsArrayType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2172),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SubscriptionDiagnosticsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SubscriptionDiagnosticsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2196),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionDiagnosticsArrayType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionDiagnosticsArrayType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2197),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionDiagnosticsVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionDiagnosticsVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2243),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionSecurityDiagnosticsArrayType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionSecurityDiagnosticsArrayType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2244),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionSecurityDiagnosticsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionSecurityDiagnosticsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11487),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OptionSetType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OptionSetType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16309),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SelectionListType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SelectionListType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17986),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AudioVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AudioVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3048),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EventTypes")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EventTypes", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 31915),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Locations")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Locations", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2253),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Server")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Server", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11312),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CurrentServerId")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CurrentServerId", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11313),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RedundantServerArray")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RedundantServerArray", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11314),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerUriArray")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerUriArray", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14415),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerNetworkGroups")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerNetworkGroups", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11192),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HistoryServerCapabilities")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HistoryServerCapabilities", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23562),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IsDeprecated")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IsDeprecated", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("Deprecates", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11737),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BitFieldMaskDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BitFieldMaskDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24263),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SemanticVersionString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SemanticVersionString", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14533),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("KeyValuePair")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("KeyValuePair", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16313),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AdditionalParametersType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AdditionalParametersType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17548),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EphemeralKeyType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EphemeralKeyType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15528),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EndpointType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EndpointType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 31917),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Handle")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Handle", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 31918),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrimmedString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrimmedString", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2299),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StateMachineType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StateMachineType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2771),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2755),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StateVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StateVariableType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 8995),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2760),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2762),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TransitionVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TransitionVariableType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2767),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2771),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FiniteStateMachineType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FiniteStateMachineType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9318),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15803),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2929),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2391),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2760),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FiniteStateVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FiniteStateVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2767),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FiniteTransitionVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FiniteTransitionVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2307),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StateType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15109),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2309),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2309),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("InitialStateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("InitialStateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2310),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TransitionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TransitionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15109),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ChoiceStateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ChoiceStateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15112),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasGuard")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasGuard", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("GuardOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15113),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("GuardVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("GuardVariableType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15317),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15128),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15128),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExpressionGuardVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExpressionGuardVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15317),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ElseGuardVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ElseGuardVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17709),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RationalNumberType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RationalNumberType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17714),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("VectorType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("VectorType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17716),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17716),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("3DVectorType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("3DVectorType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18772),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CartesianCoordinatesType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CartesianCoordinatesType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18774),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18774),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("3DCartesianCoordinatesType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("3DCartesianCoordinatesType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18779),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OrientationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OrientationType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18781),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18781),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("3DOrientationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("3DOrientationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18786),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FrameType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FrameType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18791),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18791),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("3DFrameType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("3DFrameType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18806),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RationalNumber")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RationalNumber", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18807),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Vector")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Vector", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18808),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18808),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("3DVector")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("3DVector", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18809),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CartesianCoordinates")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CartesianCoordinates", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18810),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18810),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("3DCartesianCoordinates")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("3DCartesianCoordinates", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18811),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Orientation")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Orientation", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18812),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18812),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("3DOrientation")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("3DOrientation", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18813),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Frame")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Frame", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18814),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18814),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("3DFrame")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("3DFrame", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2311),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TransitionEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TransitionEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2378),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2315),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditUpdateStateEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditUpdateStateEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3806),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11856),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11939),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OpenFileMode")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OpenFileMode", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 13353),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FileDirectoryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FileDirectoryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16314),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FileSystem")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FileSystem", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15744),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TemporaryFileTransferType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TemporaryFileTransferType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15803),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FileTransferStateMachineType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FileTransferStateMachineType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15607),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RoleSetType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RoleSetType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15620),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RoleType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RoleType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15632),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IdentityCriteriaType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IdentityCriteriaType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15634),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IdentityMappingRuleType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IdentityMappingRuleType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17641),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RoleMappingRuleChangedAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RoleMappingRuleChangedAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15644),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Anonymous")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Anonymous", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15656),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuthenticatedUser")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuthenticatedUser", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15668),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Observer")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Observer", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15680),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Operator")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Operator", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16036),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Engineer")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Engineer", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15692),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Supervisor")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Supervisor", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15716),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ConfigureAdmin")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ConfigureAdmin", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15704),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SecurityAdmin")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SecurityAdmin", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25565),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SecurityKeyServerAdmin")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SecurityKeyServerAdmin", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25603),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SecurityKeyServerAccess")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SecurityKeyServerAccess", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25584),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SecurityKeyServerPush")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SecurityKeyServerPush", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17589),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DictionaryEntryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DictionaryEntryType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17598),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17600),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17591),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DictionaryFolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DictionaryFolderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17594),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Dictionaries")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Dictionaries", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17597),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasDictionaryEntry")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasDictionaryEntry", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("DictionaryEntryOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17598),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IrdiDictionaryEntryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IrdiDictionaryEntryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17600),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UriDictionaryEntryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UriDictionaryEntryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17602),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseInterfaceType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseInterfaceType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24233),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24167),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24158),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24179),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25218),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24205),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24188),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24202),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24148),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24173),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24199),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24183),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24169),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23513),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17708),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("InterfaceTypes")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("InterfaceTypes", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17603),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasInterface")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasInterface", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("InterfaceOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17604),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasAddIn")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasAddIn", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("AddInOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23498),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CurrencyUnitType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CurrencyUnitType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23501),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CurrencyUnit")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CurrencyUnit", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23513),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IOrderedObjectType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IOrderedObjectType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23518),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OrderedListType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OrderedListType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2365),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataItemType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15318),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12021),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2372),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15318),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseAnalogType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseAnalogType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17497),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2368),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2368),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AnalogItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AnalogItemType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17570),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17497),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AnalogUnitType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AnalogUnitType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17570),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AnalogUnitRangeType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AnalogUnitRangeType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2372),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DiscreteItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DiscreteItemType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11238),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2376),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2373),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2373),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TwoStateDiscreteType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TwoStateDiscreteType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2376),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MultiStateDiscreteType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MultiStateDiscreteType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11238),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MultiStateValueDiscreteType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MultiStateValueDiscreteType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19077),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12021),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ArrayItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ArrayItemType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12068),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12047),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12057),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12029),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12038),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12029),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("YArrayItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("YArrayItemType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12038),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("XYArrayItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("XYArrayItemType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12047),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ImageItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ImageItemType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12057),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CubeItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CubeItemType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12068),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NDimensionArrayItemType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NDimensionArrayItemType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8995),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TwoStateVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TwoStateVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9002),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ConditionVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ConditionVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9004),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasTrueSubState")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasTrueSubState", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsTrueSubStateOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9005),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasFalseSubState")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasFalseSubState", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsFalseSubStateOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16361),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasAlarmSuppressionGroup")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasAlarmSuppressionGroup", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsAlarmSuppressionGroupOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16362),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmGroupMember")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmGroupMember", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("MemberOfAlarmGroup", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32059),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32059),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmSuppressionGroupMember")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmSuppressionGroupMember", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("MemberOfAlarmSuppressionGroup", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2782),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ConditionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ConditionType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2830),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2881),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2830),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DialogConditionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DialogConditionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2881),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AcknowledgeableConditionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AcknowledgeableConditionType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2915),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2915),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmConditionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmConditionType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2955),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 10523),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17080),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16405),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmGroupType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 32064),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32064),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmSuppressionGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmSuppressionGroupType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2929),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ShelvedStateMachineType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ShelvedStateMachineType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2955),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("LimitAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("LimitAlarmType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9341),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9906),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9318),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExclusiveLimitStateMachineType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExclusiveLimitStateMachineType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9341),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExclusiveLimitAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExclusiveLimitAlarmType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9623),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9482),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 9764),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9906),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NonExclusiveLimitAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NonExclusiveLimitAlarmType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 10368),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 10214),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 10060),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 10060),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NonExclusiveLevelAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NonExclusiveLevelAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9482),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExclusiveLevelAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExclusiveLevelAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 10368),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NonExclusiveDeviationAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NonExclusiveDeviationAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 10214),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NonExclusiveRateOfChangeAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NonExclusiveRateOfChangeAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9764),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExclusiveDeviationAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExclusiveDeviationAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9623),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExclusiveRateOfChangeAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExclusiveRateOfChangeAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 10523),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DiscreteAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DiscreteAlarmType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 10637),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 10637),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OffNormalAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OffNormalAlarmType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 10751),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11753),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18496),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18347),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11753),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SystemOffNormalAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SystemOffNormalAlarmType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 13225),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19297),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 10751),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TripAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TripAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18347),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("InstrumentDiagnosticAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("InstrumentDiagnosticAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18496),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SystemDiagnosticAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SystemDiagnosticAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 13225),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CertificateExpirationAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CertificateExpirationAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17080),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DiscrepancyAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DiscrepancyAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11163),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BaseConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BaseConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17219),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11165),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17220),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17218),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11166),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17221),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18665),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11164),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11164),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProcessConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProcessConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11165),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MaintenanceConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MaintenanceConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11166),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SystemConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SystemConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17218),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SafetyConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SafetyConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17219),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HighlyManagedAlarmConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HighlyManagedAlarmConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17220),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrainingConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrainingConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18665),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StatisticalConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StatisticalConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17221),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TestingConditionClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TestingConditionClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2790),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17259),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 8961),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2829),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 8944),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15013),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 8927),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17225),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 11093),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 2803),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17242),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2803),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionEnableEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionEnableEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2829),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionCommentEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionCommentEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8927),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionRespondEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionRespondEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8944),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionAcknowledgeEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionAcknowledgeEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8961),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionConfirmEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionConfirmEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11093),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionShelvingEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionShelvingEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17225),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionSuppressionEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionSuppressionEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17242),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionSilenceEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionSilenceEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15013),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionResetEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionResetEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17259),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditConditionOutOfServiceEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditConditionOutOfServiceEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2787),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RefreshStartEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RefreshStartEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2788),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RefreshEndEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RefreshEndEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2789),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RefreshRequiredEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RefreshRequiredEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 9006),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasCondition")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasCondition", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsConditionOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17276),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasEffectDisable")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasEffectDisable", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("MayBeDisabledBy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17983),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasEffectEnable")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasEffectEnable", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("MayBeEnabledBy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17984),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasEffectSuppressed")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasEffectSuppressed", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("MayBeSuppressedBy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17985),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasEffectUnsuppressed")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasEffectUnsuppressed", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("MayBeUnsuppressedBy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17279),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmMetricsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmMetricsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17277),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmRateVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmRateVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32244),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmStateVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmStateVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32251),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AlarmMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AlarmMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2391),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProgramStateMachineType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProgramStateMachineType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2378),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProgramTransitionEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProgramTransitionEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11856),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditProgramTransitionEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditProgramTransitionEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3806),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProgramTransitionAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProgramTransitionAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2380),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProgramDiagnosticType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProgramDiagnosticType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15383),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProgramDiagnostic2Type")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProgramDiagnostic2Type", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11214),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Annotations")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Annotations", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2318),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HistoricalDataConfigurationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HistoricalDataConfigurationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11202),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HA Configuration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HA Configuration", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11215),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HistoricalEventFilter")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HistoricalEventFilter", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2330),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HistoryServerCapabilitiesType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HistoryServerCapabilitiesType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2999),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditHistoryEventUpdateEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditHistoryEventUpdateEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3006),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditHistoryValueUpdateEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditHistoryValueUpdateEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19095),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditHistoryAnnotationUpdateEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditHistoryAnnotationUpdateEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3012),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditHistoryDeleteEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditHistoryDeleteEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3022),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3014),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 3019),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3014),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditHistoryRawModifyDeleteEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditHistoryRawModifyDeleteEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3019),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditHistoryAtTimeDeleteEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditHistoryAtTimeDeleteEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 3022),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuditHistoryEventDeleteEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuditHistoryEventDeleteEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12522),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrustListType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrustListType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23564),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrustListValidationOptions")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrustListValidationOptions", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12552),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrustListMasks")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrustListMasks", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12554),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrustListDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrustListDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19297),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrustListOutOfDateAlarmType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrustListOutOfDateAlarmType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12555),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CertificateGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CertificateGroupType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 13813),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CertificateGroupFolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CertificateGroupFolderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12556),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CertificateType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12558),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15181),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12557),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12557),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23537),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12559),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12560),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12558),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HttpsCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HttpsCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15181),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserCredentialCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserCredentialCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12559),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RsaMinApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RsaMinApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12560),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RsaSha256ApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RsaSha256ApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23537),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EccApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EccApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23540),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23539),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23542),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23538),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23543),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23541),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23538),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EccNistP256ApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EccNistP256ApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23539),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EccNistP384ApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EccNistP384ApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23540),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EccBrainpoolP256r1ApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EccBrainpoolP256r1ApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23541),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EccBrainpoolP384r1ApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EccBrainpoolP384r1ApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23542),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EccCurve25519ApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EccCurve25519ApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23543),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EccCurve448ApplicationCertificateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EccCurve448ApplicationCertificateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32260),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrustListUpdateRequestedAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrustListUpdateRequestedAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12561),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TrustListUpdatedAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TrustListUpdatedAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32285),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TransactionErrorType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TransactionErrorType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32286),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TransactionDiagnosticsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TransactionDiagnosticsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12581),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerConfigurationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerConfigurationType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25731),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32306),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CertificateUpdateRequestedAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CertificateUpdateRequestedAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12620),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("CertificateUpdatedAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("CertificateUpdatedAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12637),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerConfiguration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerConfiguration", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17496),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("KeyCredentialConfigurationFolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("KeyCredentialConfigurationFolderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18155),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("KeyCredentialConfiguration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("KeyCredentialConfiguration", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18001),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("KeyCredentialConfigurationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("KeyCredentialConfigurationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18011),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("KeyCredentialAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("KeyCredentialAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18029),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 18047),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18029),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("KeyCredentialUpdatedAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("KeyCredentialUpdatedAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18047),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("KeyCredentialDeletedAuditEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("KeyCredentialDeletedAuditEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23556),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuthorizationServicesConfigurationFolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuthorizationServicesConfigurationFolderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17732),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuthorizationServices")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuthorizationServices", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17852),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AuthorizationServiceConfigurationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AuthorizationServiceConfigurationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11187),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AggregateConfigurationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AggregateConfigurationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2341),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Interpolative")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Interpolative", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2342),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Average")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Average", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2343),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TimeAverage")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TimeAverage", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11285),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TimeAverage2")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TimeAverage2", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2344),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Total")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Total", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11304),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Total2")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Total2", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2346),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Minimum")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Minimum", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2347),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Maximum")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Maximum", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2348),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MinimumActualTime")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MinimumActualTime", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2349),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MaximumActualTime")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MaximumActualTime", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2350),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Range")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Range", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11286),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Minimum2")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Minimum2", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11287),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Maximum2")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Maximum2", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11305),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MinimumActualTime2")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MinimumActualTime2", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11306),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MaximumActualTime2")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MaximumActualTime2", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11288),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Range2")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Range2", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2351),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AnnotationCount")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AnnotationCount", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2352),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Count")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Count", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11307),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DurationInStateZero")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DurationInStateZero", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11308),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DurationInStateNonZero")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DurationInStateNonZero", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2355),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NumberOfTransitions")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NumberOfTransitions", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2357),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Start")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Start", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2358),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("End")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("End", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2359),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Delta")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Delta", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11505),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StartBound")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StartBound", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11506),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EndBound")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EndBound", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11507),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DeltaBounds")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DeltaBounds", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2360),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DurationGood")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DurationGood", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2361),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DurationBad")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DurationBad", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2362),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PercentGood")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PercentGood", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2363),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PercentBad")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PercentBad", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 2364),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("WorstQuality")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("WorstQuality", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11292),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("WorstQuality2")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("WorstQuality2", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11426),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StandardDeviationSample")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StandardDeviationSample", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11427),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StandardDeviationPopulation")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StandardDeviationPopulation", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11428),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("VarianceSample")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("VarianceSample", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11429),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("VariancePopulation")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("VariancePopulation", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15534),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataTypeSchemaHeader")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataTypeSchemaHeader", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14523),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15006),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14525),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataTypeDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataTypeDescription", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15488),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15487),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15005),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15487),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StructureDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StructureDescription", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15488),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EnumDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EnumDescription", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15005),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SimpleTypeDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SimpleTypeDescription", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15006),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UABinaryFileDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UABinaryFileDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24105),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PortableQualifiedName")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PortableQualifiedName", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24106),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PortableNodeId")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PortableNodeId", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24107),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UnsignedRationalNumber")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UnsignedRationalNumber", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14647),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubState")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubState", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14523),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetMetaDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetMetaDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14524),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FieldMetaData")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FieldMetaData", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15904),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetFieldFlags")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetFieldFlags", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14593),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ConfigurationVersionDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ConfigurationVersionDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15578),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedDataSetDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedDataSetDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15580),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedDataSetSourceDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedDataSetSourceDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25269),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15581),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15582),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14273),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedVariableDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedVariableDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15581),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedDataItemsDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedDataItemsDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15582),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedEventsDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedEventsDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25269),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedDataSetCustomSourceDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedDataSetCustomSourceDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15583),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetFieldContentMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetFieldContentMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15597),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetWriterDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetWriterDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15598),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetWriterTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetWriterTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15669),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15605),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetWriterMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetWriterMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15652),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15664),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15609),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubGroupDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubGroupDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15480),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15520),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15480),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("WriterGroupDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("WriterGroupDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15611),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("WriterGroupTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("WriterGroupTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15532),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15667),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15616),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("WriterGroupMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("WriterGroupMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15657),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15645),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15617),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubConnectionDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubConnectionDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15618),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ConnectionTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ConnectionTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17467),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15007),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15502),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NetworkAddressDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NetworkAddressDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15510),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15510),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NetworkAddressUrlDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NetworkAddressUrlDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15520),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReaderGroupDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReaderGroupDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15621),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReaderGroupTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReaderGroupTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15622),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReaderGroupMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReaderGroupMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15623),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetReaderDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetReaderDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15628),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetReaderTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetReaderTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15670),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23614),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15629),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetReaderMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetReaderMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15653),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15665),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15630),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SubscribedDataSetDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SubscribedDataSetDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23600),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15631),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23599),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15635),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15631),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TargetVariablesDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TargetVariablesDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14744),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FieldTargetDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FieldTargetDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15874),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OverrideValueHandling")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OverrideValueHandling", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15635),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SubscribedDataSetMirrorDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SubscribedDataSetMirrorDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15530),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubConfigurationDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubConfigurationDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23602),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23599),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StandaloneSubscribedDataSetRefDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StandaloneSubscribedDataSetRefDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23600),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StandaloneSubscribedDataSetDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StandaloneSubscribedDataSetDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23601),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SecurityGroupDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SecurityGroupDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25270),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubKeyPushTargetDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubKeyPushTargetDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23602),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubConfiguration2DataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubConfiguration2DataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 20408),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetOrderingType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetOrderingType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15642),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UadpNetworkMessageContentMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UadpNetworkMessageContentMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15645),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UadpWriterGroupMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UadpWriterGroupMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15646),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UadpDataSetMessageContentMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UadpDataSetMessageContentMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15652),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UadpDataSetWriterMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UadpDataSetWriterMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15653),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UadpDataSetReaderMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UadpDataSetReaderMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15654),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("JsonNetworkMessageContentMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("JsonNetworkMessageContentMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15657),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("JsonWriterGroupMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("JsonWriterGroupMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15658),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("JsonDataSetMessageContentMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("JsonDataSetMessageContentMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15664),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("JsonDataSetWriterMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("JsonDataSetWriterMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15665),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("JsonDataSetReaderMessageDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("JsonDataSetReaderMessageDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23603),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("QosDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("QosDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23604),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23608),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23604),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TransmitQosDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TransmitQosDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23605),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23605),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TransmitQosPriorityDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TransmitQosPriorityDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23608),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReceiveQosDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReceiveQosDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23609),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23609),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReceiveQosPriorityDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReceiveQosPriorityDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17467),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DatagramConnectionTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DatagramConnectionTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23612),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23612),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DatagramConnectionTransport2DataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DatagramConnectionTransport2DataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15532),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DatagramWriterGroupTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DatagramWriterGroupTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 23613),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23613),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DatagramWriterGroupTransport2DataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DatagramWriterGroupTransport2DataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23614),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DatagramDataSetReaderTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DatagramDataSetReaderTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15007),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerConnectionTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerConnectionTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15008),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerTransportQualityOfService")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerTransportQualityOfService", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15667),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerWriterGroupTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerWriterGroupTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15669),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerDataSetWriterTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerDataSetWriterTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15670),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerDataSetReaderTransportDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerDataSetReaderTransportDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15906),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubKeyServiceType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubKeyServiceType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14416),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15452),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SecurityGroupFolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SecurityGroupFolderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15471),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SecurityGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SecurityGroupType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25345),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasPushedSecurityGroup")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasPushedSecurityGroup", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("HasPushTarget", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25337),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubKeyPushTargetType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubKeyPushTargetType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25346),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubKeyPushTargetFolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubKeyPushTargetFolderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14416),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishSubscribeType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishSubscribeType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14443),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishSubscribe")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishSubscribe", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32405),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetClasses")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetClasses", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14476),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasPubSubConnection")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasPubSubConnection", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("PubSubConnectionOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25482),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubConfigurationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubConfigurationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25517),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubConfigurationRefMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubConfigurationRefMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25519),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubConfigurationRefDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubConfigurationRefDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25520),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubConfigurationValueDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubConfigurationValueDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14509),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedDataSetType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedDataSetType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14534),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 14572),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15489),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExtensionFieldsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExtensionFieldsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14936),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetToWriter")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetToWriter", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("WriterToDataSet", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14534),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedDataItemsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedDataItemsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14572),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PublishedEventsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PublishedEventsType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14477),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetFolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetFolderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14209),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubConnectionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubConnectionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17721),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ConnectionTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ConnectionTransportType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15064),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15155),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14232),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubGroupType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17999),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 17725),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17725),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("WriterGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("WriterGroupType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15296),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasDataSetWriter")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasDataSetWriter", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsWriterInGroup", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18804),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasWriterGroup")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasWriterGroup", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsWriterGroupOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17997),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("WriterGroupTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("WriterGroupTransportType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21133),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21136),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17998),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("WriterGroupMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("WriterGroupMessageType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21105),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21126),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17999),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReaderGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReaderGroupType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15297),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasDataSetReader")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasDataSetReader", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsReaderInGroup", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18805),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasReaderGroup")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasReaderGroup", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsReaderGroupOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21090),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReaderGroupTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReaderGroupTransportType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21091),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReaderGroupMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReaderGroupMessageType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15298),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetWriterType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetWriterType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15305),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetWriterTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetWriterTransportType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21138),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21096),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetWriterMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetWriterMessageType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21111),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21128),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15306),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetReaderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetReaderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15319),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetReaderTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetReaderTransportType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21142),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24016),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21104),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataSetReaderMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataSetReaderMessageType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21130),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21116),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15108),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SubscribedDataSetType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SubscribedDataSetType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15127),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15111),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15111),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TargetVariablesType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TargetVariablesType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15127),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SubscribedDataSetMirrorType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SubscribedDataSetMirrorType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23795),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SubscribedDataSetFolderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SubscribedDataSetFolderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23828),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StandaloneSubscribedDataSetType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StandaloneSubscribedDataSetType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14643),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubStatusType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubStatusType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19677),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19903),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19732),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19786),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19834),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19968),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 20027),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19723),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DiagnosticsLevel")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DiagnosticsLevel", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19725),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsCounterType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsCounterType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19730),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsCounterClassification")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsCounterClassification", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19732),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsRootType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsRootType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19786),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsConnectionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsConnectionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19834),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsWriterGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsWriterGroupType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19903),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsReaderGroupType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsReaderGroupType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19968),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsDataSetWriterType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsDataSetWriterType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 20027),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubDiagnosticsDataSetReaderType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubDiagnosticsDataSetReaderType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23832),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubCapabilitiesType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubCapabilitiesType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15535),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubStatusEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubStatusEventType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15563),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 15548),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15548),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubTransportLimitsExceedEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubTransportLimitsExceedEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15563),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PubSubCommunicationFailureEventType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PubSubCommunicationFailureEventType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21105),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UadpWriterGroupMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UadpWriterGroupMessageType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21111),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UadpDataSetWriterMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UadpDataSetWriterMessageType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21116),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UadpDataSetReaderMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UadpDataSetReaderMessageType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21126),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("JsonWriterGroupMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("JsonWriterGroupMessageType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21128),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("JsonDataSetWriterMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("JsonDataSetWriterMessageType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21130),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("JsonDataSetReaderMessageType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("JsonDataSetReaderMessageType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15064),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DatagramConnectionTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DatagramConnectionTransportType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21133),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DatagramWriterGroupTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DatagramWriterGroupTransportType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24016),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DatagramDataSetReaderTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DatagramDataSetReaderTransportType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15155),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerConnectionTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerConnectionTransportType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21136),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerWriterGroupTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerWriterGroupTransportType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21138),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerDataSetWriterTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerDataSetWriterTransportType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21142),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BrokerDataSetReaderTransportType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BrokerDataSetReaderTransportType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21145),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NetworkAddressType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NetworkAddressType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 21147),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21147),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NetworkAddressUrlType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NetworkAddressUrlType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23455),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AliasNameType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AliasNameType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23456),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AliasNameCategoryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AliasNameCategoryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23468),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AliasNameDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AliasNameDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23469),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AliasFor")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AliasFor", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("HasAlias", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23470),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Aliases")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Aliases", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23479),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TagVariables")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TagVariables", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23488),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Topics")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Topics", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24264),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserManagementType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserManagementType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24277),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PasswordOptionsMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PasswordOptionsMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24279),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserConfigurationMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserConfigurationMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24281),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserManagementDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserManagementDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24290),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserManagement")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserManagement", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19077),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MultiStateDictionaryEntryDiscreteBaseType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MultiStateDictionaryEntryDiscreteBaseType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 19084),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19084),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MultiStateDictionaryEntryDiscreteType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MultiStateDictionaryEntryDiscreteType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25726),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EncodedTicket")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EncodedTicket", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25731),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ApplicationConfigurationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ApplicationConfigurationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 26871),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProvisionableDeviceType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProvisionableDeviceType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 29878),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProvisionableDevice")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProvisionableDevice", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24148),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIetfBaseNetworkInterfaceType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIetfBaseNetworkInterfaceType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24158),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeBaseEthernetPortType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeBaseEthernetPortType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24233),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeAutoNegotiationStatusType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeAutoNegotiationStatusType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24167),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IBaseEthernetCapabilitiesType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IBaseEthernetCapabilitiesType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25218),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IVlanIdType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IVlanIdType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24169),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ISrClassType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ISrClassType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24173),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeBaseTsnStreamType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeBaseTsnStreamType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24179),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeBaseTsnTrafficSpecificationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeBaseTsnTrafficSpecificationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24183),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeBaseTsnStatusStreamType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeBaseTsnStatusStreamType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24188),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeTsnInterfaceConfigurationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeTsnInterfaceConfigurationType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24191),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 24195),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24191),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeTsnInterfaceConfigurationTalkerType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeTsnInterfaceConfigurationTalkerType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24195),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeTsnInterfaceConfigurationListenerType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeTsnInterfaceConfigurationListenerType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24199),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeTsnMacAddressType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeTsnMacAddressType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24202),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IIeeeTsnVlanTagType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IIeeeTsnVlanTagType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24205),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IPriorityMappingEntryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IPriorityMappingEntryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24210),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Duplex")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Duplex", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24212),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("InterfaceAdminStatus")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("InterfaceAdminStatus", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24214),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("InterfaceOperStatus")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("InterfaceOperStatus", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24216),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NegotiationStatus")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NegotiationStatus", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24218),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TsnFailureCode")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TsnFailureCode", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24220),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TsnStreamState")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TsnStreamState", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24222),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TsnTalkerStatus")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TsnTalkerStatus", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24224),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TsnListenerStatus")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TsnListenerStatus", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25220),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PriorityMappingEntryType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PriorityMappingEntryType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24226),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Resources")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Resources", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24227),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Communication")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Communication", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24228),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MappingTables")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MappingTables", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24229),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NetworkInterfaces")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NetworkInterfaces", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24230),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Streams")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Streams", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24231),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TalkerStreams")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TalkerStreams", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24232),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ListenerStreams")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ListenerStreams", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25221),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IetfBaseNetworkInterfaceType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IetfBaseNetworkInterfaceType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25227),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObjectType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PriorityMappingTableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PriorityMappingTableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25237),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UsesPriorityMappingTable")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UsesPriorityMappingTable", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("UsedByNetworkInterface", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25238),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasLowerLayerInterface")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasLowerLayerInterface", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("HasHigherLayerInterface", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25253),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IsExecutableOn")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IsExecutableOn", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("CanExecute", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25254),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Controls")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Controls", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsControlledBy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25255),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Utilizes")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Utilizes", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsUtilizedBy", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25261),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25265),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25265),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IsExecutingOn")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IsExecutingOn", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("Executes", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25256),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Requires")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Requires", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("IsRequiredBy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25257),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IsPhysicallyConnectedTo")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IsPhysicallyConnectedTo", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25258),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RepresentsSameEntityAs")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RepresentsSameEntityAs", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25260),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25259),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25259),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RepresentsSameHardwareAs")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RepresentsSameHardwareAs", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25260),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RepresentsSameFunctionalityAs")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RepresentsSameFunctionalityAs", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25261),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IsHostedBy")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IsHostedBy", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("Hosts", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25262),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasPhysicalComponent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasPhysicalComponent", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("PhysicalComponentOf", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25264),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 25263),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25263),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasContainedComponent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasContainedComponent", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("ContainedComponentOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25264),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasAttachedComponent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasAttachedComponent", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("AttachedComponentOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32679),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassReferenceType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HasReferenceDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HasReferenceDescription", "")),
				ua.AttributeIDInverseName: ua.MustVariant(attrs.InverseName("ReferenceDescriptionOf", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32657),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariableType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReferenceDescriptionVariableType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReferenceDescriptionVariableType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32659),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReferenceDescriptionDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReferenceDescriptionDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32660),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ReferenceListEntryDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ReferenceListEntryDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 256),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IdType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IdType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 257),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NodeClass")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NodeClass", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 94),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PermissionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PermissionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15031),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AccessLevelType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AccessLevelType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15406),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AccessLevelExType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AccessLevelExType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15033),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EventNotifierType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EventNotifierType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 95),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AccessRestrictionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AccessRestrictionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 96),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RolePermissionType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RolePermissionType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 97),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DataTypeDefinition")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DataTypeDefinition", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 99),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 100),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 98),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StructureType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StructureType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 101),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StructureField")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StructureField", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 99),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StructureDefinition")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StructureDefinition", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 100),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EnumDefinition")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EnumDefinition", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 296),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Argument")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Argument", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 7594),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EnumValueType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EnumValueType", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 102),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 102),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EnumField")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EnumField", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12755),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("OptionSet")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("OptionSet", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12877),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NormalizedString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NormalizedString", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12878),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DecimalString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DecimalString", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12879),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DurationString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DurationString", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12880),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TimeString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TimeString", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12881),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DateString")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DateString", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 290),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Duration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Duration", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 294),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UtcTime")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UtcTime", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 295),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("LocaleId")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("LocaleId", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8912),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("TimeZoneDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("TimeZoneDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17588),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Index")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Index", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 288),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IntegerId")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IntegerId", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 307),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ApplicationType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ApplicationType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 308),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ApplicationDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ApplicationDescription", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 20998),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("VersionTime")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("VersionTime", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12189),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerOnNetwork")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerOnNetwork", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 311),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ApplicationInstanceCertificate")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ApplicationInstanceCertificate", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 302),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MessageSecurityMode")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MessageSecurityMode", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 303),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserTokenType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserTokenType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 304),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserTokenPolicy")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserTokenPolicy", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 312),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EndpointDescription")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EndpointDescription", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 432),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RegisteredServer")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RegisteredServer", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12890),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DiscoveryConfiguration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DiscoveryConfiguration", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 12891),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12891),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MdnsDiscoveryConfiguration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MdnsDiscoveryConfiguration", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 315),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SecurityTokenRequestType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SecurityTokenRequestType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 344),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SignedSoftwareCertificate")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SignedSoftwareCertificate", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 388),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionAuthenticationToken")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionAuthenticationToken", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 316),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserIdentityToken")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserIdentityToken", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 319),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 938),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 325),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 322),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 319),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AnonymousIdentityToken")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AnonymousIdentityToken", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 322),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("UserNameIdentityToken")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("UserNameIdentityToken", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 325),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("X509IdentityToken")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("X509IdentityToken", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 938),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("IssuedIdentityToken")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("IssuedIdentityToken", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 348),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NodeAttributesMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NodeAttributesMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 376),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AddNodesItem")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AddNodesItem", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 379),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AddReferencesItem")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AddReferencesItem", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 382),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DeleteNodesItem")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DeleteNodesItem", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 385),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DeleteReferencesItem")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DeleteReferencesItem", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 347),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AttributeWriteMask")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AttributeWriteMask", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 521),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ContinuationPoint")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ContinuationPoint", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 537),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RelativePathElement")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RelativePathElement", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 540),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RelativePath")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RelativePath", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 289),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Counter")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Counter", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 291),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NumericRange")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NumericRange", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 331),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EndpointConfiguration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EndpointConfiguration", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 576),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FilterOperator")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FilterOperator", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 583),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ContentFilterElement")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ContentFilterElement", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 586),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ContentFilter")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ContentFilter", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 589),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("FilterOperand")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("FilterOperand", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 595),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 598),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 592),
					IsForward:       true,
				},
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 601),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 592),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ElementOperand")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ElementOperand", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 595),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("LiteralOperand")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("LiteralOperand", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 598),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AttributeOperand")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AttributeOperand", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 601),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SimpleAttributeOperand")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SimpleAttributeOperand", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 659),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HistoryEvent")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HistoryEvent", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11234),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HistoryUpdateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HistoryUpdateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11293),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("PerformUpdateType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("PerformUpdateType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 719),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("MonitoringFilter")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("MonitoringFilter", "")),
			},
			[]*ua.ReferenceDescription{
				{
					ReferenceTypeID: ua.NewNumericNodeID(0, id.HasSubtype),
					TypeDefinition:  ua.NewNumericExpandedNodeID(0, 725),
					IsForward:       true,
				},
			},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 725),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EventFilter")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EventFilter", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 948),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AggregateConfiguration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AggregateConfiguration", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 920),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("HistoryEventFieldList")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("HistoryEventFieldList", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 338),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("BuildInfo")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("BuildInfo", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 851),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RedundancySupport")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RedundancySupport", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 852),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerState")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerState", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 853),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("RedundantServerDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("RedundantServerDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11943),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EndpointUrlListDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EndpointUrlListDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11944),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("NetworkGroupDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("NetworkGroupDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 856),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SamplingIntervalDiagnosticsDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SamplingIntervalDiagnosticsDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 859),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerDiagnosticsSummaryDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerDiagnosticsSummaryDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 862),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServerStatusDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServerStatusDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 865),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionDiagnosticsDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionDiagnosticsDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 868),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SessionSecurityDiagnosticsDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SessionSecurityDiagnosticsDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 871),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ServiceCounterDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ServiceCounterDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 299),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("StatusResult")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("StatusResult", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 874),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SubscriptionDiagnosticsDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SubscriptionDiagnosticsDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 877),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ModelChangeStructureDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ModelChangeStructureDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 897),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("SemanticChangeStructureDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("SemanticChangeStructureDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 884),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Range")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Range", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 887),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("EUInformation")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("EUInformation", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12077),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AxisScaleEnumeration")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AxisScaleEnumeration", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12171),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ComplexNumberType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ComplexNumberType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12172),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("DoubleComplexNumberType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("DoubleComplexNumberType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12079),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("AxisInformation")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("AxisInformation", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12080),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("XVType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("XVType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 894),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProgramDiagnosticDataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProgramDiagnosticDataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24033),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ProgramDiagnostic2DataType")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ProgramDiagnostic2DataType", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 891),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Annotation")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Annotation", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 890),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassDataType)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("ExceptionDeviationFormat")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("ExceptionDeviationFormat", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12766),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14846),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17537),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17549),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15671),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18815),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18816),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18817),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18818),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18819),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18820),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18821),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18822),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18823),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15736),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23507),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12680),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32382),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15676),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 125),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 126),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 127),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15421),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15422),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24108),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24109),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24110),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 124),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14839),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14847),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15677),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15678),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14323),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15679),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15681),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25529),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15682),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15683),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15688),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15689),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21150),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15691),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15693),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15694),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15695),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21151),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21152),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21153),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15701),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15702),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15703),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15705),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15706),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15707),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15712),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14848),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15713),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21154),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23851),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23852),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23853),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25530),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23854),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15715),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15717),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15718),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15719),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15724),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15725),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23855),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23856),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23857),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23860),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23861),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17468),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23864),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21155),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23865),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23866),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15479),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15727),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15729),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15733),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25531),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25532),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23499),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24292),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25239),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32661),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32662),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 128),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 121),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14844),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 122),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 123),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 298),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8251),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14845),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12765),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8917),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 310),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12207),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 306),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 314),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 434),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12900),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12901),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 346),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 318),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 321),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 324),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 327),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 940),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 378),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 381),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 384),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 387),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 539),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 542),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 333),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 585),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 588),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 591),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 594),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 597),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 600),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 603),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 661),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 721),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 727),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 950),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 922),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 340),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 855),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11957),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11958),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 858),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 861),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 864),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 867),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 870),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 873),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 301),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 876),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 879),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 899),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 886),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 889),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12181),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12182),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12089),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12090),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 896),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24034),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 893),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default Binary")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default Binary", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 7617),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Opc.Ua")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Opc.Ua", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12758),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14802),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17541),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17553),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15949),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18851),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18852),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18853),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18854),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18855),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18856),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18857),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18858),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 18859),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15728),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23520),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12676),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32386),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15950),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14796),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15589),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15590),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15529),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15531),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24120),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24121),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24122),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14794),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14795),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14803),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15951),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15952),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14319),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15953),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15954),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25545),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15955),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15956),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15987),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15988),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21174),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15990),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15991),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15992),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15993),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21175),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21176),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21177),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15995),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15996),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16007),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16008),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16009),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16010),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16011),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14804),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16012),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21178),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23919),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23920),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23921),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25546),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23922),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16014),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16015),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16016),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16017),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16018),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16019),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23923),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23924),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23925),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23928),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23929),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17472),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23932),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21179),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23933),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23934),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15579),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16021),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16022),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16023),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25547),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25548),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23505),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24296),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25243),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32669),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32670),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16126),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14797),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14800),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14798),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14799),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 297),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 7616),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 14801),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12757),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8913),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 309),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12195),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 305),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 313),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 433),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12892),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12893),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 345),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 317),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 320),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 323),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 326),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 939),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 377),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 380),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 383),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 386),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 538),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 541),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 332),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 584),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 587),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 590),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 593),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 596),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 599),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 602),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 660),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 720),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 726),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 949),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 921),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 339),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 854),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11949),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 11950),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 857),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 860),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 863),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 866),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 869),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 872),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 300),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 875),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 878),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 898),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 885),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 888),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12173),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12174),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12081),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 12082),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 895),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24038),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 892),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default XML")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default XML", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 8252),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassVariable)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Opc.Ua")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Opc.Ua", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15085),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15041),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17547),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17557),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16150),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19064),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19065),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19066),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19067),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19068),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19069),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19070),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19071),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 19072),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15042),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23528),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15044),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32390),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16151),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15057),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15058),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15059),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15700),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15714),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24132),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24133),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24134),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15050),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15051),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15049),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16152),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16153),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15060),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16154),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16155),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25561),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16156),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16157),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16158),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16159),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21198),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16161),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16280),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16281),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16282),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21199),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21200),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21201),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16284),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16285),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16286),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16287),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16288),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16308),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16310),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15061),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16311),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21202),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23987),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23988),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23989),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25562),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23990),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16323),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16391),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16392),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16393),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16394),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16404),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23991),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23992),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23993),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23996),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23997),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 17476),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24000),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 21203),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24001),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24002),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15726),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16524),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16525),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 16526),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25563),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25564),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 23511),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24300),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 25247),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32677),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 32678),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15062),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15063),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15065),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15066),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15067),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15081),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15082),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15083),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15084),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15086),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15087),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15095),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15098),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15099),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15102),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15105),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15106),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15136),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15140),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15141),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15142),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15143),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15144),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15165),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15169),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15172),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15175),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15188),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15189),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15199),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15204),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15205),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15206),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15207),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15208),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15209),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15210),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15273),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15293),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15295),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15304),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15349),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15361),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15362),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15363),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15364),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15365),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15366),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15367),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15368),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15369),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15370),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15371),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15372),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15373),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15374),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15375),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15376),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15377),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15378),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15379),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15380),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15381),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 24042),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
		NewNode(
			ua.NewNumericNodeID(0, 15382),
			map[ua.AttributeID]*ua.Variant{
				ua.AttributeIDNodeClass:   ua.MustVariant(uint32(ua.NodeClassObject)),
				ua.AttributeIDBrowseName:  ua.MustVariant(attrs.BrowseName("Default JSON")),
				ua.AttributeIDDisplayName: ua.MustVariant(attrs.DisplayName("Default JSON", "")),
			},
			[]*ua.ReferenceDescription{},
			nil,
		),
	}
}
