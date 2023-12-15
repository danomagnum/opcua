// Copyright 2018-2020 opcua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package main

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gopcua/opcua/debug"
	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"
)

var (
	endpoint = flag.String("endpoint", "opc.tcp://0.0.0.0:4840", "OPC UA Endpoint URL")
	certfile = flag.String("cert", "cert.pem", "Path to certificate file")
	keyfile  = flag.String("key", "key.pem", "Path to PEM Private Key file")
	gencert  = flag.Bool("gen-cert", false, "Generate a new certificate")
)

func main() {
	flag.BoolVar(&debug.Enable, "debug", false, "enable debug logging")
	flag.Parse()
	log.SetFlags(0)

	var opts []server.Option

	opts = append(opts,
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableSecurity("Basic128Rsa15", ua.MessageSecurityModeSign),
		server.EnableSecurity("Basic128Rsa15", ua.MessageSecurityModeSignAndEncrypt),
		server.EnableSecurity("Basic256", ua.MessageSecurityModeSign),
		server.EnableSecurity("Basic256", ua.MessageSecurityModeSignAndEncrypt),
		server.EnableSecurity("Basic256Sha256", ua.MessageSecurityModeSignAndEncrypt),
		server.EnableSecurity("Basic256Sha256", ua.MessageSecurityModeSign),
		server.EnableSecurity("Aes128_Sha256_RsaOaep", ua.MessageSecurityModeSign),
		server.EnableSecurity("Aes128_Sha256_RsaOaep", ua.MessageSecurityModeSignAndEncrypt),
		server.EnableSecurity("Aes256_Sha256_RsaPss", ua.MessageSecurityModeSign),
		server.EnableSecurity("Aes256_Sha256_RsaPss", ua.MessageSecurityModeSignAndEncrypt),
	)

	opts = append(opts,
		server.EnableAuthMode(ua.UserTokenTypeAnonymous),
		server.EnableAuthMode(ua.UserTokenTypeUserName),
		server.EnableAuthMode(ua.UserTokenTypeCertificate),
		//		server.EnableAuthWithoutEncryption(), // Dangerous and not recommended, shown for illustration only
	)

	var cert []byte
	if *gencert || (*certfile != "" && *keyfile != "") {
		debug.Printf("Loading cert/key from %s/%s", *certfile, *keyfile)
		c, err := tls.LoadX509KeyPair(*certfile, *keyfile)
		if err != nil {
			log.Printf("Failed to load certificate: %s", err)
		} else {
			pk, ok := c.PrivateKey.(*rsa.PrivateKey)
			if !ok {
				log.Fatalf("Invalid private key")
			}
			cert = c.Certificate[0]
			opts = append(opts, server.PrivateKey(pk), server.Certificate(cert))
		}
	}

	s := server.New(*endpoint, opts...)

	// Create some namespaces backed by go map[string]any
	mrw := server.NewMapNamespace(s, "MyTestNamespace")
	mrw2 := server.NewMapNamespace(s, "SomeOtherNamespace")

	// fill them with data.
	mrw.Data["Tag1"] = 123.4
	mrw.Data["Tag2"] = 42
	mrw.Data["Tag3.Tag4"] = "some string"
	mrw.Data["Tag5"] = true
	mrw.Data["Tag6"] = time.Now()

	mrw2.Data["Tag7"] = 56.78
	mrw2.Data["Tag8"] = 92
	mrw2.Data["Tag9"] = "different string"
	mrw2.Data["Tag10"] = false
	mrw2.Data["Tag11"] = time.Now().Add(time.Hour)

	// simulate a background process updating the map
	go func() {
		updates := 0
		num := 42
		tag5 := true
		time.Sleep(time.Second * 10)
		for {
			updates++
			num++
			// you can manually lock and change the value, then manually trigger the change notification
			mrw.Mu.Lock()
			mrw.Data["Tag2"] = num
			s.ChangeNotification(ua.NewStringNodeID(mrw.ID(), "Tag2"))
			mrw.Mu.Unlock()
			if updates == 10 {
				// or you can do it with the built-in functions.
				// which handles the locking and triggering
				tag5 = !tag5
				mrw.SetValue("Tag5", tag5)
				updates = 0
			}
			time.Sleep(time.Second)
		}
	}()

	// add the namespaces to the server, and add a reference to them
	root_ns, _ := s.Namespace(0)
	root_obj := root_ns.Objects()

	mrw_id := s.AddNamespace(mrw)
	root_obj.AddRef(mrw.Objects())
	log.Printf("map namespace added at index %d", mrw_id)
	mrw_id2 := s.AddNamespace(mrw2)
	root_obj.AddRef(mrw2.Objects())
	log.Printf("map namespace added at index %d", mrw_id2)

	// Start the server
	if err := s.Start(context.Background()); err != nil {
		log.Fatalf("Error starting server, exiting: %s", err)
	}
	defer s.Close()

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)
	defer signal.Stop(sigch)

	// Create a new node namespace
	nodeNS := server.NewNodeNameSpace(s, "NodeNamespace")
	// add it to the server.
	s.AddNamespace(nodeNS)
	// Create some nodes for it.
	var1 := nodeNS.AddNewVariableNode("TestVar1", float32(123.45))
	// Make sure there is a reference to the variable from the root object folder
	nns_obj := nodeNS.Objects()
	nns_obj.AddRef(var1)

	// add the reference for this namespace's root object folder to the server's root object folder
	root_obj.AddRef(nns_obj)

	log.Printf("Press CTRL-C to exit")
	<-sigch
	log.Printf("Shutting down the server...")
}
